from datetime import datetime
from typing import Self

from config import config
from modules.redis.schema import RedisKey, Metadata, WorkerConfig, WorkerMetric, ChunkInfo
from modules.redis.connect import get_redis_connection
from modules.redis.data_types import Set, Stack, SortedSet


# functools.lru_cache is not used because it does not support expiration based on time
class ExpiringData:
    def __init__(self, data, expire_time=60):
        self.data = data
        self.expire_time = expire_time
        self.fetched_at = datetime.now()

    @property
    def is_expired(self):
        return (datetime.now() - self.fetched_at).seconds > self.expire_time


class RedisData:
    def __init__(self, migration_id, use_cached_property=True):
        self.conn = get_redis_connection()
        self.cluster_id = config.SOURCE_CLUSTER_ID
        self.migration_id = migration_id
        self.use_cached_property = use_cached_property

        self.updated_pk_set: SortedSet = SortedSet(self.get_key(RedisKey.UPDATED_PK_SET), self.conn)
        self.removed_pk_set: SortedSet = SortedSet(self.get_key(RedisKey.REMOVED_PK_SET), self.conn)
        self.chunk_stack: Stack = Stack(self.get_key(RedisKey.CHUNK_STACK), self.conn)
        self.chunk_set: Set = Set(self.get_key(RedisKey.CHUNK_SET), self.conn)

    @staticmethod
    def cached_property(expire_time=60):
        def decorator(func):
            @property
            def wrapper(self: Self):
                if not self.use_cached_property:
                    return func(self)
                cached_data = getattr(self, f'_{func.__name__}', None)
                if cached_data is None or cached_data.is_expired:
                    cached_data = ExpiringData(func(self), expire_time)
                    setattr(self, f'_{func.__name__}', cached_data)
                return cached_data.data

            return wrapper

        return decorator

    def get_key(self, fstring, *args):
        return fstring.format(self.cluster_id, self.migration_id, *args)

    @property
    def current_stage(self):
        return self.conn.get(self.get_key(RedisKey.CURRENT_STAGE))

    def set_current_stage(self, stage):
        self.conn.set(self.get_key(RedisKey.CURRENT_STAGE), stage)

    @property
    def last_catchup_timestamp(self):
        last_catchup_timestamp = self.conn.get(self.get_key(RedisKey.LAST_CATCHUP_TIMESTAMP))
        last_catchup_timestamp = float(last_catchup_timestamp) if last_catchup_timestamp else 1
        return last_catchup_timestamp

    def set_last_catchup_timestamp(self, status):
        self.conn.set(self.get_key(RedisKey.LAST_CATCHUP_TIMESTAMP), status)

    @cached_property(60)
    def metadata(self) -> Metadata:
        return Metadata(self.get_key(RedisKey.METADATA))

    @property
    def worker_config(self) -> WorkerConfig:
        return WorkerConfig(self.get_key(RedisKey.WORKER_CONFIG))

    @property
    def worker_metric(self) -> WorkerMetric:
        return WorkerMetric(self.get_key(RedisKey.WORKER_METRIC, env.POD_NAME))

    def get_all_worker_metrics(self):
        worker_metric_keys = self.conn.keys(self.get_key(RedisKey.WORKER_METRIC, '*'))
        return [WorkerMetric(key) for key in worker_metric_keys]

    def push_chunk(self, *chunk_ids):
        self.chunk_set.add(*chunk_ids)
        self.chunk_stack.push(*chunk_ids)

    def get_chunk_info(self, chunk_id) -> ChunkInfo:
        return ChunkInfo(self.get_key(RedisKey.CHUNK_INFO, chunk_id))

    def remove_all_chunks(self):
        chunk_ids = self.chunk_set.getall()
        for chunk_id in chunk_ids:
            self.conn.delete(self.get_key(RedisKey.CHUNK_INFO, chunk_id))
        self.chunk_set.delete()
        self.chunk_stack.delete()

    @property
    def old_source_table(self):
        return self.conn.get(self.get_key(RedisKey.OLD_SOURCE_TABLE))

    def set_old_source_table(self, table_name):
        if table_name is None:
            self.conn.delete(self.get_key(RedisKey.OLD_SOURCE_TABLE))
        else:
            self.conn.set(self.get_key(RedisKey.OLD_SOURCE_TABLE), table_name)
