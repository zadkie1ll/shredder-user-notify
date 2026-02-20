import logging
import inspect
import proto.rwmanager_pb2 as proto
from datetime import datetime, timedelta
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from common.models.db import User, NcUsersNotification


class NcUsersFinder:
    def __init__(self, session_maker: async_sessionmaker):
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.__session_maker = session_maker

    async def __get_notified(self, session: AsyncSession) -> set[int]:
        notified_users = await session.execute(
            select(User.telegram_id).join(
                NcUsersNotification, User.id == NcUsersNotification.user_id
            )
        )

        return set(row[0] for row in notified_users.fetchall())

    async def find(self, users: list[proto.UserResponse]) -> set[int]:
        try:
            self.__logger.info("searching not connected users...")

            result = set()

            async with self.__session_maker() as session:
                notified = await self.__get_notified(session=session)

            self.__logger.info(
                f"found {len(notified)} not connected created yesterday users already notified"
            )

            for user in users:
                if not user.HasField("created_at"):
                    continue

                created_at = user.created_at.ToDatetime()

                now = datetime.now()
                is_user_created_yesterday = (
                    now.date() - created_at.date()
                ) == timedelta(days=1)

                dont_handle_user = (
                    not is_user_created_yesterday
                    or user.lifetime_used_traffic_bytes > 0
                )

                if dont_handle_user:
                    continue

                tg_id = int(user.username)

                if tg_id in notified:
                    continue

                self.__logger.info(
                    f"user {user.username} created_at={created_at}, no traffic, will be notified"
                )
                result.add(tg_id)

            self.__logger.info(
                f"{len(result)} users require 'not connected yesterday' notifications"
            )
            return result
        except Exception:
            self.__logger.exception(f"error in {inspect.currentframe().f_code.co_name}")
            return set()
