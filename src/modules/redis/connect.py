from typing import Literal

import redis
from redis.backoff import ExponentialBackoff
from redis.retry import Retry

from config import secret


def get_redis_connection(decode_responses: Literal[True, False] = True):
    conn_args = {
        'port': 6379,
        'decode_responses': decode_responses,
        'retry': Retry(ExponentialBackoff(10, 1), 3),
        'retry_on_error': [ConnectionError, TimeoutError, ConnectionRefusedError],
    }
    conn = redis.Redis(
        host=secret.REDIS_HOST,
        password=secret.REDIS_PASSWORD,
        **conn_args
    )
    return conn
