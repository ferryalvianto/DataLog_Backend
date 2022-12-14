
import os
from functools import lru_cache
from kombu import Queue


def route_task(name, args, kwargs, options, task=None, **kw):
    if ":" in name:
        queue, _ = name.split(":")
        return {"queue": queue}
    return {"queue": "celery"}


class BaseConfig:
    # broker_url: str = os.environ.get("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672//")
    broker_url: str = os.environ.get("CELERY_BROKER_URL", "amqps://yonaaaww:KJZVILlOh3aofNKPjbfm4u2hm9G1FiKt@shark.rmq.cloudamqp.com/yonaaaww")
    result_backend: str = os.environ.get("CELERY_RESULT_BACKEND", "rpc://")

    CELERY_TASK_QUEUES: list = (
        # default queue
        Queue("celery"),
        # custom queue
        Queue("tasks")
    )

    CELERY_TASK_ROUTES = (route_task,)


class DevelopmentConfig(BaseConfig):
    pass


@lru_cache()
def get_settings():
    config_cls_dict = {
        "development": DevelopmentConfig,
    }
    config_name = os.environ.get("CELERY_CONFIG", "development")
    config_cls = config_cls_dict[config_name]
    return config_cls()


settings = get_settings()
