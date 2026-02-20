import os

MI_UN_LOG_LEVEL = "MI_UN_LOG_LEVEL"
MI_UN_SYNC_USER_PROGRESS = "MI_UN_SYNC_USER_PROGRESS"
MI_UN_LOOP_INTERVAL = "MI_UN_LOOP_INTERVAL"

# MBMS
MI_UN_RWMS_ADDR = "MI_UN_RWMS_ADDR"
MI_UN_RWMS_PORT = "MI_UN_RWMS_PORT"

# postgres
MI_UN_POSTGRES_HOST = "MI_UN_POSTGRES_HOST"
MI_UN_POSTGRES_PORT = "MI_UN_POSTGRES_PORT"
MI_UN_POSTGRES_USER = "MI_UN_POSTGRES_USER"
MI_UN_POSTGRES_PASSWORD = "MI_UN_POSTGRES_PASSWORD"
MI_UN_POSTGRES_DB = "MI_UN_POSTGRES_DB"

# redis
MI_UN_REDIS_HOST = "MI_UN_REDIS_HOST"
MI_UN_REDIS_PORT = "MI_UN_REDIS_PORT"
MI_UN_REDIS_PASSWORD = "MI_UN_REDIS_PASSWORD"


class Config:
    def __init__(self):
        self.log_level: str = os.getenv(MI_UN_LOG_LEVEL)

        if not self.log_level:
            self.log_level = "info"

        sync_flag = os.getenv(MI_UN_SYNC_USER_PROGRESS)
        self.sync_user_progress: bool = (
            True if sync_flag and sync_flag.lower() == "true" else False
        )

        loop_interval = os.getenv(MI_UN_LOOP_INTERVAL)

        if not loop_interval:
            self.loop_interval: int = 30
        else:
            self.loop_interval: int = int(loop_interval)

        # mbms envs
        self.rwms_address: str = os.getenv(MI_UN_RWMS_ADDR)
        self.rwms_port: int = int(os.getenv(MI_UN_RWMS_PORT))

        if not self.rwms_address:
            raise ValueError(f"{MI_UN_RWMS_ADDR} environment variable is not set.")

        if not self.rwms_port:
            raise ValueError(f"{MI_UN_RWMS_PORT} environment variable is not set.")

        # redis envs
        self.redis_host: str = os.getenv(MI_UN_REDIS_HOST)
        self.redis_port: int = int(os.getenv(MI_UN_REDIS_PORT))
        self.redis_password: str = os.getenv(MI_UN_REDIS_PASSWORD)

        if not self.redis_host:
            raise ValueError(f"{MI_UN_REDIS_HOST} environment variable is not set.")

        if not self.redis_port:
            raise ValueError(f"{MI_UN_REDIS_PORT} environment variable is not set.")

        if not self.redis_password:
            raise ValueError(f"{MI_UN_REDIS_PASSWORD} environment variable is not set.")

        # postgres envs
        self.pg_host: str = os.getenv(MI_UN_POSTGRES_HOST)
        self.pg_port: int = int(os.getenv(MI_UN_POSTGRES_PORT))
        self.pg_user: str = os.getenv(MI_UN_POSTGRES_USER)
        self.pg_password: str = os.getenv(MI_UN_POSTGRES_PASSWORD)
        self.pg_db: str = os.getenv(MI_UN_POSTGRES_DB)

        if not self.pg_host:
            raise ValueError(f"{MI_UN_POSTGRES_HOST} environment variable is not set.")

        if not self.pg_port:
            raise ValueError(f"{MI_UN_POSTGRES_PORT} environment variable is not set.")

        if not self.pg_user:
            raise ValueError(f"{MI_UN_POSTGRES_USER} environment variable is not set.")

        if not self.pg_password:
            raise ValueError(
                f"{MI_UN_POSTGRES_PASSWORD} environment variable is not set."
            )

        if not self.pg_db:
            raise ValueError(f"{MI_UN_POSTGRES_DB} environment variable is not set.")
