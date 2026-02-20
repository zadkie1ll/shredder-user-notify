import orjson
import logging
from redis.asyncio import Redis
from config import Config
from common.models.messages import MessageUnion


class RedisPublisher:
    def __init__(self, config: Config):
        self.__logger = logging.getLogger(self.__class__.__name__)

        self.__redis = Redis(
            host=config.redis_host,
            port=config.redis_port,
            password=config.redis_password,
            decode_responses=True,
        )
        self.__logger.info(
            f"connected to Redis at {config.redis_host}:{config.redis_port}"
        )

    async def push_message_to_vpn_bot(self, message: MessageUnion):
        try:
            data = message.model_dump()
            json = orjson.dumps(data).decode("utf-8")
            await self.__redis.rpush("monkey-island-vpn-bot", json)
            self.__logger.debug(f"message pushed: {data}")
        except Exception:
            self.__logger.exception("failed to push message to Redis")
            raise

    async def push_message_to_vps_bot(self, message: MessageUnion):
        try:
            data = message.model_dump()
            json = orjson.dumps(data).decode("utf-8")
            await self.__redis.rpush("monkey-island-vps-bot", json)
            self.__logger.debug(f"message pushed: {data}")
        except Exception:
            self.__logger.exception("failed to push message to Redis")
            raise

    async def push_message_to_ym_stat(self, message: MessageUnion):
        try:
            data = message.model_dump()
            json = orjson.dumps(data).decode("utf-8")
            await self.__redis.rpush("monkey-island-ym-stat", json)
            self.__logger.debug(f"message pushed: {data}")
        except Exception:
            self.__logger.exception("failed to push message to Redis")
            raise
