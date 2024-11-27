from datetime import datetime

from modules.redis.data_types import Hash


class ChunkInfo(Hash):
    start_pk: int
    end_pk: int
    status: str
    last_pk_inserted: int


class WorkerConfig(Hash):
    batch_size: int
    thread_count: int
    commit_interval: int
    revision: int


# Metrics managed by WorkerManager
class WorkerMetric(Hash):
    average_insert_rate: float


class Metadata(Hash):
    source_db: str
    source_table: str
    destination_db: str
    destination_table: str
    source_columns: str
    pk_column: str
    max_pk: int
    start_datetime: datetime


class RedisKey:
    METADATA = 'sb-osc:{}:{}:metadata'
    CURRENT_STAGE = 'sb-osc:{}:{}:current_stage'
    LAST_CATCHUP_TIMESTAMP = 'sb-osc:{}:{}:last_catchup_timestamp'
    UPDATED_PK_SET = 'sb-osc:{}:{}:updated_pk_set'
    REMOVED_PK_SET = 'sb-osc:{}:{}:removed_pk_set'
    WORKER_CONFIG = 'sb-osc:{}:{}:worker_config'
    WORKER_METRIC = 'sb-osc:{}:{}:worker_metrics:{}'
    CHUNK_STACK = 'sb-osc:{}:{}:bulk_import:chunk_stack'
    CHUNK_SET = 'sb-osc:{}:{}:bulk_import:chunk_set'
    CHUNK_INFO = 'sb-osc:{}:{}:bulk_import:chunk_info:{}'
    OLD_SOURCE_TABLE = 'sb-osc:{}:{}:old_source_table'
