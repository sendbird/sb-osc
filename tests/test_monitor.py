import time

import pytest

from config import config
from modules.db import Database
from sbosc.const import Stage
from sbosc.monitor import MetricMonitor


class DatabaseTest(Database):
    def get_instance_id(self, host='source', role='writer'):
        return 'test'


class MetricMonitorTest(MetricMonitor):
    def __init__(self):
        super().__init__()
        self.db = DatabaseTest()

    def get_writer_cpu(self, writer_id):
        return 30

    def get_write_latency(self, writer_id):
        return 5


############
# Fixtures #
############
@pytest.fixture
def monitor(redis_data):
    redis_data.worker_config.batch_size = config.MIN_BATCH_SIZE
    redis_data.worker_config.thread_count = config.MIN_THREAD_COUNT
    redis_data.worker_config.commit_interval = 0.01
    redis_data.worker_config.revision = 0
    return MetricMonitorTest()


########
# Test #
########
def get_metric_names(monitor):
    return set([
        metric.name for metric in
        monitor.metric_sender.registry.collect()
        if metric.samples
    ])


def test_update_worker_config(monitor, redis_data):
    redis_data.set_current_stage(Stage.BULK_IMPORT)

    batch_size = config.MIN_BATCH_SIZE
    thread_count = config.MIN_THREAD_COUNT

    # first update
    redis_data.worker_metric.average_insert_rate = 100
    monitor.update_worker_config()
    assert redis_data.worker_config.batch_size == batch_size + config.BATCH_SIZE_STEP_SIZE
    assert redis_data.worker_config.thread_count == thread_count
    assert monitor.previous_batch_size == batch_size
    batch_size += config.BATCH_SIZE_STEP_SIZE

    metric_set = get_metric_names(monitor)
    assert metric_set == {
        'sb_osc_worker_batch_size',
        'sb_osc_worker_thread_count',
        'sb_osc_worker_commit_interval',
        'sb_osc_average_insert_rate'
    }

    # insert rate increased with new batch_size
    redis_data.worker_metric.average_insert_rate = 200
    monitor.update_worker_config()
    assert redis_data.worker_config.batch_size == batch_size + config.BATCH_SIZE_STEP_SIZE
    assert redis_data.worker_config.thread_count == thread_count
    assert monitor.previous_batch_size == batch_size

    # insert rate not increased with new batch_size
    redis_data.worker_metric.average_insert_rate = 200
    monitor.update_worker_config()
    assert redis_data.worker_config.batch_size == batch_size
    assert redis_data.worker_config.thread_count == thread_count + config.THREAD_COUNT_STEP_SIZE
    thread_count += config.THREAD_COUNT_STEP_SIZE

    # insert rate increased with new thread count
    redis_data.worker_metric.average_insert_rate = 300
    monitor.update_worker_config()
    assert redis_data.worker_config.batch_size == batch_size + config.BATCH_SIZE_STEP_SIZE
    assert redis_data.worker_config.thread_count == thread_count

    # insert rate not increased with new batch_size, max thread count reached
    redis_data.worker_metric.average_insert_rate = 200
    monitor.update_worker_config()
    assert redis_data.worker_config.batch_size == batch_size
    assert redis_data.worker_config.thread_count == thread_count

    # update triggerd with max thread count, but insert rate not increased
    redis_data.worker_metric.average_insert_rate = 200
    monitor.update_worker_config()
    assert monitor.optimal_batch_size == batch_size
    assert monitor.optimal_thread_count == thread_count

    for _ in range(config.OPTIMAL_VALUE_USE_LIMIT):
        monitor.update_worker_config()

    assert monitor.optimal_batch_size is None
    assert redis_data.worker_config.batch_size == batch_size + config.BATCH_SIZE_STEP_SIZE
    assert redis_data.worker_config.thread_count == thread_count


def test_check_migration_status(monitor, cursor, redis_data):
    cursor.execute("TRUNCATE TABLE sbosc.event_handler_status")
    cursor.execute("TRUNCATE TABLE sbosc.apply_dml_events_status")
    monitor.redis_data.metadata.max_id = 0
    monitor.check_migration_status()
    metric_set = get_metric_names(monitor)
    expected_metrics = {
        'sb_osc_updated_pk_set_length',
        'sb_osc_removed_pk_set_length',
        'sb_osc_unmatched_rows',
        'sb_osc_remaining_binlog_size'
    }
    assert metric_set == expected_metrics

    monitor.redis_data.metadata.max_id = 100
    monitor.check_migration_status()
    metric_set = get_metric_names(monitor)
    expected_metrics.add('sb_osc_bulk_import_progress')
    assert metric_set == expected_metrics

    cursor.execute(f'''
        INSERT INTO sbosc.event_handler_status (migration_id, log_file, log_pos, last_event_timestamp, created_at)
        VALUES (1, 'mysql-bin.000001', 4, 2, NOW())
    ''')
    redis_data.set_last_catchup_timestamp(time.time())
    monitor.check_migration_status()
    metric_set = get_metric_names(monitor)
    expected_metrics = expected_metrics | {
        'sb_osc_last_catchup_timestamp',
        'sb_osc_last_event_timestamp',
    }
    assert metric_set == expected_metrics

    cursor.execute(f'''
        INSERT INTO sbosc.apply_dml_events_status (migration_id, last_loaded_timestamp, created_at)
        VALUES (1, NOW(), NOW())
    ''')
    monitor.check_migration_status()
    metric_set = get_metric_names(monitor)
    expected_metrics.add('sb_osc_last_loaded_timestamp')
    assert metric_set == expected_metrics
