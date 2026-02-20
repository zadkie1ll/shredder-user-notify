import logging
import inspect
from sqlalchemy import select, text, delete, func
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from common.models.db import User, ExpiredUsersNotification


class ExpiredUsersFinder:
    def __init__(self, session_maker: async_sessionmaker):
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.__session_maker = session_maker

    async def __get_expired_users(self, session: AsyncSession) -> set[int]:
        expired = await session.execute(
            select(User.telegram_id).where(
                User.expire_at.isnot(None),
                func.now() - User.expire_at >= text("INTERVAL '0 secs'"),
            )
        )

        return set(row[0] for row in expired.fetchall())

    async def __get_notified(self, session: AsyncSession) -> set[int]:
        notified = await session.execute(
            select(User.telegram_id).join(
                ExpiredUsersNotification, ExpiredUsersNotification.user_id == User.id
            )
        )

        return set(row[0] for row in notified.fetchall())

    async def __validate_expired_users_notifications(
        self, session: AsyncSession
    ) -> None:
        self.__logger.info(
            "validated expired users notifications table (removed non-expired)"
        )

        no_longer_expired_users = select(User.id).where(
            User.expire_at.isnot(None),
            User.expire_at - func.now() > text("INTERVAL '0 secs'"),
        )

        await session.execute(
            delete(ExpiredUsersNotification).where(
                ExpiredUsersNotification.user_id.in_(no_longer_expired_users)
            )
        )

    async def find(self) -> set[int]:
        try:
            async with self.__session_maker() as session:
                async with session.begin():
                    self.__logger.info("starting search for expired users to notify")
                    await self.__validate_expired_users_notifications(session=session)

                    expired_users = await self.__get_expired_users(session=session)
                    self.__logger.info(f"found {len(expired_users)} expired users")

                    notified = await self.__get_notified(session=session)
                    self.__logger.info(
                        f"found {len(notified)} already notified expired users"
                    )

                    result = expired_users - notified
                    self.__logger.info(
                        f"{len(result)} expired users require notifications"
                    )

            return result
        except Exception:
            self.__logger.exception(f"error in {inspect.currentframe().f_code.co_name}")
            return set()
