import time
import asyncio
import logging
import proto.rwmanager_pb2 as proto

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from common.setup_logger import setup_logger
from common.rwms_client import RwmsClient
from common.models.db import User
from common.models.db import EventLog
from common.models.messages import ConversionEvent
from common.models.messages import NotificateUserMessage
from common.models.messages import SendConversionMessage
from common.models.messages import ReferralReachedTrafficBonusApplied
from common.models.analytics_event import AnalyticsEvent
from common.models.analytics_event import TrafficThresholdReached

from config import Config
from redis_publisher import RedisPublisher
from nc_users_finder import NcUsersFinder
from expired_users_finder import ExpiredUsersFinder
from user_traffic_progress_watcher import UserTrafficProgressWatcher
from subscription_expiration_finder import SubscriptionExpirationFinder
from subscription_expiration_finder import NotifyAboutSubscriptionExpiration


async def save_event_log(
    session: AsyncSession, event: AnalyticsEvent, username: str
) -> None:
    user_id = await session.scalar(
        select(User.id).where(User.username == username).limit(1)
    )

    if user_id is None:
        logging.error(f"not fount user id for telegram id {event.telegram_id}")
        return

    session.add(
        EventLog(
            user_id=user_id,
            event_type=event.event_type,
            event_payload=event.model_dump(),
        )
    )


async def save_traffic_threshold_reached_event_log(
    session: AsyncSession, username: str, threshold: int
):
    event = TrafficThresholdReached(threshold=threshold)
    await save_event_log(session, event, username)


async def send_bonuses_applied_messages(
    bonuses_applied: dict[int, int], publisher: RedisPublisher
):
    for referrer_id, referral_count in bonuses_applied.items():
        message = ReferralReachedTrafficBonusApplied(
            telegram_id=referrer_id,
            referral_reached_traffic_count=referral_count,
            bonus_days_count=referral_count * 10,
        )
        await publisher.push_message_to_vpn_bot(message)
        await publisher.push_message_to_vps_bot(message)


async def send_subscription_expiration_notifications(
    to: NotifyAboutSubscriptionExpiration, publisher: RedisPublisher
):
    for tg_id in to.one_day_left:
        message = NotificateUserMessage(
            service="monkey-island-vpn-bot",
            type="notificate-user",
            notification_type="1-day-left",
            telegram_id=tg_id,
        )

        await publisher.push_message_to_vpn_bot(message)
        await publisher.push_message_to_vps_bot(message)

    for tg_id in to.three_days_left:
        message = NotificateUserMessage(
            service="monkey-island-vpn-bot",
            type="notificate-user",
            notification_type="3-days-left",
            telegram_id=tg_id,
        )

        await publisher.push_message_to_vpn_bot(message)
        await publisher.push_message_to_vps_bot(message)


async def send_not_connected_notifications(to: set[int], publisher: RedisPublisher):
    for tg_id in to:
        message = NotificateUserMessage(
            service="monkey-island-vpn-bot",
            type="notificate-user",
            notification_type="nc-yesterday-created",
            telegram_id=tg_id,
        )

        await publisher.push_message_to_vpn_bot(message)
        await publisher.push_message_to_vps_bot(message)


async def send_expired_users_notifications(to: set[int], publisher: RedisPublisher):
    for tg_id in to:
        message = NotificateUserMessage(
            service="monkey-island-vpn-bot",
            type="notificate-user",
            notification_type="subscription-expired",
            telegram_id=tg_id,
        )

        await publisher.push_message_to_vpn_bot(message)
        await publisher.push_message_to_vps_bot(message)


async def send_traffic_ym_conversion_event(
    username: str, event: ConversionEvent, publisher: RedisPublisher
):
    try:
        logging.info(f"sending {event} conversion for {username}")

        msg = SendConversionMessage(
            service="monkey-island-ym-stat",
            type="send-conversion",
            client_id=username,
            event=event,
        )

        await publisher.push_message_to_ym_stat(message=msg)
        logging.info(f"{event} conversion for {username} has been sent")

    except Exception:
        logging.exception(f"pushing {event} YM conversions task error for {username}")


async def rwms_get_all_users(rwms: RwmsClient) -> list[proto.UserResponse]:
    try:
        offset = 0
        count = 1000
        response = await rwms.get_all_users(offset=offset, count=count)

        result: list = list()
        result.extend(response.users)

        while len(result) < response.total:
            offset += count
            response = await rwms.get_all_users(offset=offset, count=count)
            result.extend(response.users)

        return result
    except Exception as e:
        logging.error(f"error fetching users from RWMS: {e}")
        return []


async def main(
    config: Config,
    session_maker: async_sessionmaker,
    publisher: RedisPublisher,
) -> None:
    rwms = RwmsClient(addr=config.rwms_address, port=config.rwms_port)

    user_traffic_progress_watcher = UserTrafficProgressWatcher(
        sync_user_progress=config.sync_user_progress, session_maker=session_maker
    )

    nc_users_finder = NcUsersFinder(session_maker=session_maker)
    expired_users_finder = ExpiredUsersFinder(session_maker=session_maker)
    subscription_expiration_finder = SubscriptionExpirationFinder(
        session_maker=session_maker
    )

    logging.info("user notification service started")
    logging.info(
        f"DB: {config.pg_host}:{config.pg_port}/{config.pg_db}, Redis: {config.redis_host}:{config.redis_port}"
    )
    logging.info(f"loop interval: {config.loop_interval}m")

    while True:
        start_time = time.monotonic()

        try:
            # subscription expiration
            t0 = time.monotonic()
            telegram_ids = await subscription_expiration_finder.find()

            logging.info(
                f"found {len(telegram_ids.one_day_left)} users with 1-day-left and "
                f"{len(telegram_ids.three_days_left)} with 3-days-left (took {time.monotonic()-t0:.2f}s)"
            )

            await send_subscription_expiration_notifications(
                to=telegram_ids, publisher=publisher
            )

            # expired users
            t0 = time.monotonic()
            expired = await expired_users_finder.find()

            logging.info(
                f"found {len(expired)} expired users (took {time.monotonic()-t0:.2f}s)"
            )
            await send_expired_users_notifications(to=expired, publisher=publisher)

            all_users = await rwms_get_all_users(rwms)

            if len(all_users) == 0:
                logging.info("failed to fetch active users from RWMS")
                continue

            logging.info(f"fetched {len(all_users)} active users from RWMS")

            if not len(all_users):
                continue

            # not connected users
            t0 = time.monotonic()
            nc_users_to_notify = await nc_users_finder.find(users=all_users)

            logging.info(
                f"found {len(nc_users_to_notify)} not connected users (took {time.monotonic()-t0:.2f}s)"
            )
            await send_not_connected_notifications(
                to=nc_users_to_notify, publisher=publisher
            )

            # user traffic progress check
            conversions_to_send, bonuses_applied = (
                await user_traffic_progress_watcher.find(rwms, all_users)
            )

            # Отправляем уведомление о бонусах
            await send_bonuses_applied_messages(
                bonuses_applied=bonuses_applied, publisher=publisher
            )

            for username, event in conversions_to_send:
                await send_traffic_ym_conversion_event(
                    username=username, event=event, publisher=publisher
                )

            async with session_maker() as session:
                async with session.begin():
                    try:
                        for username, event in conversions_to_send:
                            if event == ConversionEvent.HAS_TRAFFIC:
                                await save_traffic_threshold_reached_event_log(
                                    session, username, 0
                                )
                            if event == ConversionEvent.HAS_TRAFFIC_MORE_THAN_5MB:
                                await save_traffic_threshold_reached_event_log(
                                    session, username, 5
                                )
                            if event == ConversionEvent.HAS_TRAFFIC_MORE_THAN_100MB:
                                await save_traffic_threshold_reached_event_log(
                                    session, username, 100
                                )
                    except Exception as e:
                        logging.error(f"save event log error: {e}")

        except (asyncio.CancelledError, KeyboardInterrupt):
            logging.info("received shutdown signal")
            return
        except Exception:
            logging.exception(f"main iteration error")

        duration = time.monotonic() - start_time
        logging.info(f"iteration finished in {duration:.2f}s")

        await asyncio.sleep(60 * config.loop_interval)


if __name__ == "__main__":
    config = Config()

    log_level = logging.INFO

    if config.log_level.lower() == "debug":
        log_level = logging.DEBUG
    if config.log_level.lower() == "info":
        log_level = logging.INFO
    if config.log_level.lower() == "warning":
        log_level = logging.WARN
    if config.log_level.lower() == "error":
        log_level = logging.ERROR
    if config.log_level.lower() == "critical":
        log_level = logging.CRITICAL

    setup_logger(filename="monkey-island-user-notify.log", level=log_level)

    publisher = RedisPublisher(config=config)

    db_url = f"postgresql+asyncpg://{config.pg_user}:{config.pg_password}@{config.pg_host}:{config.pg_port}/{config.pg_db}"
    engine = create_async_engine(db_url, echo=False)
    session_maker = async_sessionmaker(bind=engine, expire_on_commit=False)

    try:
        asyncio.run(
            main(config=config, session_maker=session_maker, publisher=publisher)
        )
    except KeyboardInterrupt as e:
        logging.error("interrupted by user")
