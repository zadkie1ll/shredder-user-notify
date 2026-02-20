import logging
from pydantic import BaseModel
from typing import Set
from sqlalchemy import func, text, select, delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from common.models.db import User, ExtendSubscriptionNotification, YkRecurrentPayment


class NotifyAboutSubscriptionExpiration(BaseModel):
    one_day_left: Set[int] = set()
    three_days_left: Set[int] = set()


class SubscriptionExpirationFinder:
    def __init__(self, session_maker: async_sessionmaker):
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.__session_maker = session_maker

    async def find(self) -> NotifyAboutSubscriptionExpiration:
        self.__logger.info("starting search for users with expiring subscriptions")

        ESN = ExtendSubscriptionNotification

        async with self.__session_maker() as session:
            async with session.begin():
                await self.__clear_extend_subscription_notifications_table(
                    session=session
                )
                users_1day_left = await self.__get_1day_left_users(session)
                self.__logger.info(
                    f"found {len(users_1day_left)} users with 1 day left"
                )

                users_3days_left = await self.__get_3days_left_users(session)
                self.__logger.info(
                    f"found {len(users_3days_left)} users with 3 days left"
                )

                users_with_recurrent = await self.__get_tg_ids_with_recurrent(session)
                self.__logger.info(
                    f"found {len(users_with_recurrent)} users with recurrent payments"
                )

                unique_telegram_ids = users_1day_left | users_3days_left
                self.__logger.info(
                    f"total unique users to check for notifications: {len(unique_telegram_ids)}"
                )

                if not unique_telegram_ids:
                    return NotifyAboutSubscriptionExpiration()

                user_ids_statement = select(User.id).where(
                    User.telegram_id.in_(unique_telegram_ids)
                )

                already_notified_result = await session.execute(
                    select(User.telegram_id, ESN.one_day_before, ESN.three_days_before)
                    .join(User, User.id == ESN.user_id)
                    .where(ESN.user_id.in_(user_ids_statement))
                )

                for (
                    telegram_id,
                    one_day_left,
                    three_days_left,
                ) in already_notified_result:
                    if one_day_left:
                        users_1day_left.discard(telegram_id)

                    if three_days_left:
                        users_3days_left.discard(telegram_id)

                for telegram_id in users_with_recurrent:
                    users_1day_left.discard(telegram_id)
                    users_3days_left.discard(telegram_id)

                self.__logger.info(
                    f"after filtering already notified & recurrent: {len(users_1day_left)} (1d) and "
                    f"{len(users_3days_left)} (3d)"
                )

                return NotifyAboutSubscriptionExpiration(
                    one_day_left=users_1day_left, three_days_left=users_3days_left
                )

    async def __get_1day_left_users(self, session: AsyncSession) -> set[int]:
        result = await session.execute(
            select(User.telegram_id).where(
                User.expire_at.isnot(None),
                User.expire_at - func.now() <= text("INTERVAL '1 day'"),
                User.expire_at - func.now() > text("INTERVAL '0 seconds'"),
            )
        )

        return set(row[0] for row in result.fetchall())

    async def __get_3days_left_users(self, session: AsyncSession) -> set[int]:
        result = await session.execute(
            select(User.telegram_id).where(
                User.expire_at.isnot(None),
                User.expire_at - func.now() <= text("INTERVAL '3 days'"),
                User.expire_at - func.now() > text("INTERVAL '2 days'"),
            )
        )

        return set(row[0] for row in result.fetchall())

    async def __get_tg_ids_with_recurrent(self, session: AsyncSession) -> set[int]:
        result = await session.execute(
            select(User.telegram_id).join(
                YkRecurrentPayment, User.id == YkRecurrentPayment.user_id
            )
        )

        return set(row[0] for row in result.fetchall())

    async def __clear_extend_subscription_notifications_table(
        self, session: AsyncSession
    ):
        """
        Старый запрос был таким:

        DELETE FROM extend_subscription_notifications esn
        USING users u
        WHERE
            esn.user_id = u.id AND
            u.expire_at IS NOT NULL AND
            u.expire_at - NOW() > INTERVAL '3 days'

        """

        long_subscriptions = select(User.id).where(
            User.expire_at.isnot(None),
            User.expire_at - func.now() > text("INTERVAL '3 days'"),
        )

        await session.execute(
            delete(ExtendSubscriptionNotification).where(
                ExtendSubscriptionNotification.user_id.in_(long_subscriptions)
            )
        )

        self.__logger.info(
            f"cleared {ExtendSubscriptionNotification.__tablename__} table for long-term subscriptions"
        )
