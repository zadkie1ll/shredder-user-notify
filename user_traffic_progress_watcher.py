import logging
import proto.rwmanager_pb2 as proto
from datetime import timezone
from datetime import datetime
from datetime import timedelta
from typing import Tuple
from typing import Optional
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from common.rwms_client import RwmsClient
from common.models.db import User
from common.models.db import UserTrafficProgress
from common.models.db import ReferralBonus
from common.models.db import ReferralBonusType
from common.models.db import ReferralType
from common.models.messages import ConversionEvent

K5MB = 5 * 1024 * 1024
K100MB = 100 * 1024 * 1024
K15MINUTES = 15 * 60


async def extend_user_subscription_by_username(
    session: AsyncSession,
    username: str,
    interval: timedelta,
) -> None:
    extend_expire_at_query = text("""
        UPDATE users 
            SET expire_at = 
            CASE 
                WHEN expire_at > (NOW() AT TIME ZONE 'UTC') THEN expire_at + (:interval)::interval
                ELSE (NOW() AT TIME ZONE 'UTC') + (:interval)::interval
            END
            WHERE username = :username
        """)

    await session.execute(
        extend_expire_at_query,
        {
            "username": username,
            "interval": interval,
        },
    )


async def update_user(
    rwms_client: RwmsClient,
    squads_uuids: list[str],
    user: proto.UserResponse,
    interval: timedelta,
) -> Tuple[Optional[proto.UserResponse], bool]:
    new_expire_at = None
    subscription_activated = False

    if user.HasField("expire_at"):
        new_expire_at = user.expire_at.ToDatetime(tzinfo=timezone.utc)

    if new_expire_at is None or new_expire_at < datetime.now(timezone.utc):
        new_expire_at = datetime.now(timezone.utc) + interval
        subscription_activated = True
    else:
        new_expire_at = new_expire_at + interval

    update_user_response = await rwms_client.update_user(
        proto.UpdateUserRequest(
            uuid=user.uuid,
            expire_at=new_expire_at,
            status=proto.UserStatus.ACTIVE,
            traffic_limit_strategy=proto.TrafficLimitStrategy.NO_RESET,
            active_internal_squads=[*squads_uuids],
        )
    )

    return update_user_response, subscription_activated


class UserTrafficProgressDto(BaseModel):
    user_id: int
    passed_0: bool
    passed_5mb: bool
    passed_100mb: bool


class UserTrafficProgressWatcher:
    def __init__(self, sync_user_progress: bool, session_maker: async_sessionmaker):
        self.__session_maker = session_maker
        self.__sync_user_progress = sync_user_progress
        self.__log = logging.getLogger(self.__class__.__name__)

    # returns a list of tuples of (telegram_id, ConversionEvent)
    async def find(
        self, rwms_client: RwmsClient, users: list[proto.UserResponse]
    ) -> tuple[list[tuple[str, ConversionEvent]], dict[int, int]]:
        self.__log.info(f"searching user traffic progress...")

        try:
            if self.__sync_user_progress:
                self.__log.info(
                    f"synchronizing {UserTrafficProgress.__tablename__} table..."
                )
                self.__sync_user_progress = False
                await self.__update_user_progress(users)

            async with self.__session_maker() as session:
                user_progress = await self.__get_users_progress(session=session)

            users_to_update = []  # tuples of (user_id, column name, value)
            conversions_to_send = []  # tuples of (username, ConversionEvent)
            referrer_bonuses = (
                []
            )  # list of user ids of referrers with standard referral type

            for user in users:
                if user.username not in user_progress:
                    continue

                progress: UserTrafficProgressDto = user_progress[user.username]
                traffic: float = user.lifetime_used_traffic_bytes

                if traffic > 0 and not progress.passed_0:
                    users_to_update.append((progress.user_id, "passed_0", True))
                    conversions_to_send.append(
                        (user.username, ConversionEvent.HAS_TRAFFIC)
                    )

                if traffic > K5MB and not progress.passed_5mb:
                    users_to_update.append((progress.user_id, "passed_5mb", True))
                    conversions_to_send.append(
                        (user.username, ConversionEvent.HAS_TRAFFIC_MORE_THAN_5MB)
                    )

                if traffic > K100MB and not progress.passed_100mb:
                    referrer_bonuses.append(progress.user_id)
                    users_to_update.append((progress.user_id, "passed_100mb", True))
                    conversions_to_send.append(
                        (user.username, ConversionEvent.HAS_TRAFFIC_MORE_THAN_100MB)
                    )

            bonus_result = {}

            async with self.__session_maker() as session:
                async with session.begin():
                    for user_id, column, value in users_to_update:
                        # Этот запрос можно оптимизировать через .where(UserTrafficProgress.user_id._in())
                        await session.execute(
                            update(UserTrafficProgress)
                            .where(UserTrafficProgress.user_id == user_id)
                            .values({column: value})
                        )

                    if referrer_bonuses:
                        bonus_result = await self.__add_bonuses_if_needed(
                            session, rwms_client, referrer_bonuses
                        )

            self.__log.info(
                f"updated traffic progress for {len(users_to_update)} users"
            )

            return conversions_to_send, bonus_result

        except Exception:
            self.__log.exception(f"error in {self.__class__.__name__}")

    async def __update_user_progress(self, users):
        try:
            async with self.__session_maker() as session:
                async with session.begin():
                    for user in users:
                        traffic = user.lifetime_used_traffic_bytes
                        passed_0 = traffic > 0
                        passed_5mb = traffic > K5MB
                        passed_100mb = traffic > K100MB

                        # SQLAlchemy core approach
                        insert_stmt = """
                            INSERT INTO user_traffic_progress (user_id, passed_0, passed_5mb, passed_100mb)
                            SELECT id, :passed_0, :passed_5mb, :passed_100mb 
                            FROM users 
                            WHERE telegram_id = :telegram_id
                            ON CONFLICT (user_id) DO NOTHING
                        """

                        self.__log.info(
                            f"insert user {user.username} to user_traffic_progress"
                        )

                        await session.execute(
                            text(insert_stmt),
                            {
                                "telegram_id": int(user.username),
                                "passed_0": passed_0,
                                "passed_5mb": passed_5mb,
                                "passed_100mb": passed_100mb,
                            },
                        )
        except Exception as e:
            self.__log.exception(
                f"Failed to update progress for user {user.username}: {e}"
            )

    async def __get_users_progress(
        self, session: AsyncSession
    ) -> dict[str, UserTrafficProgressDto]:
        result = await session.execute(
            select(
                User.username,
                UserTrafficProgress.user_id,
                UserTrafficProgress.passed_0,
                UserTrafficProgress.passed_5mb,
                UserTrafficProgress.passed_100mb,
            ).join(User, User.id == UserTrafficProgress.user_id)
        )

        rows = result.fetchall()

        return {
            username: UserTrafficProgressDto(
                user_id=user_id,
                passed_0=passed_0,
                passed_5mb=passed_5mb,
                passed_100mb=passed_100mb,
            )
            for username, user_id, passed_0, passed_5mb, passed_100mb in rows
        }

    # Возвращает словарь, состоящий из количества рефералов достигших 100мб трафика для каждого реферера
    async def __add_bonuses_if_needed(
        self, session: AsyncSession, rwms_client: RwmsClient, user_ids: list[int]
    ) -> dict[int, int]:
        # 1. Получаем рефералов (user_ids) и их рефереров
        referrals_with_referrers = await session.execute(
            select(User.id, User.referred_by_id)
            .where(User.id.in_(user_ids))
            .where(User.referral_type == ReferralType.STANDARD)
            .where(User.referred_by_id.isnot(None))  # только те, у кого есть реферер
        )

        # 2. Собираем данные о рефералах
        referral_data = [
            (row[0], row[1]) for row in referrals_with_referrers
        ]  # [(referral_id, referrer_id), ...]

        if not referral_data:
            return {}

        # 3. Получаем ID рефералов, у которых УЖЕ ЕСТЬ TRAFFIC бонус
        existing_bonuses = await session.execute(
            select(ReferralBonus.referral_id)
            .where(ReferralBonus.referral_id.in_([r[0] for r in referral_data]))
            .where(ReferralBonus.bonus_type == ReferralBonusType.TRAFFIC)
        )

        existing_referral_ids = set(existing_bonuses.scalars().all())

        # 4. Оставляем только тех рефералов, у которых еще НЕТ TRAFFIC бонуса
        new_referrals = [
            (ref_id, ref_by_id)
            for ref_id, ref_by_id in referral_data
            if ref_id not in existing_referral_ids
        ]

        if not new_referrals:
            return {}

        # 5. Группируем по реферерам для удобства
        referrals_by_referrer = {}
        for referral_id, referrer_id in new_referrals:
            if referrer_id not in referrals_by_referrer:
                referrals_by_referrer[referrer_id] = []
            referrals_by_referrer[referrer_id].append(referral_id)

        result = await session.execute(
            select(User.id, User.username, User.telegram_id).where(
                User.id.in_(referrals_by_referrer.keys())
            )
        )

        referrer_data = {
            user_id: (username, telegram_id)
            for user_id, username, telegram_id in result.all()
        }

        bonus_result: dict[int, int] = {}  # telegram_id -> количество рефералов

        # 6. Начисляем бонусы
        for referrer_id, referral_ids in referrals_by_referrer.items():
            referrer_info = referrer_data.get(referrer_id)

            if not referrer_info:
                self.__log.warning(f"not found {referrer_id} in referrer_data")
                continue

            referrer_username, referrer_tg_id = referrer_info

            referrer_sub = await rwms_client.get_user_by_username(referrer_username)

            if not referrer_sub:
                self.__log.warning(
                    f"not found remnawave subscription for {referrer_username}"
                )
                continue

            # Создаем записи о бонусе для каждого реферала
            for referral_id in referral_ids:
                bonus = ReferralBonus(
                    referral_id=referral_id,
                    referrer_id=referrer_id,
                    bonus_type=ReferralBonusType.TRAFFIC,
                    days_added=10,  # или сколько дней за TRAFFIC
                )
                session.add(bonus)
                bonus_result[referrer_tg_id] = bonus_result.get(referrer_tg_id, 0) + 1

            total_days = len(referral_ids) * 10
            bonus_interval = timedelta(days=total_days)

            await extend_user_subscription_by_username(
                session, referrer_username, bonus_interval
            )

            await update_user(
                rwms_client,
                [s.uuid for s in referrer_sub.active_internal_squads],
                referrer_sub,
                bonus_interval,
            )

        return bonus_result
