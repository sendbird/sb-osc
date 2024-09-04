import time
from datetime import datetime, timedelta
from typing import Tuple

from MySQLdb.cursors import DictCursor, Cursor
from prometheus_client import Gauge, Counter, CollectorRegistry, start_http_server

from config import config
from sbosc.component import SBOSCComponent
from sbosc.const import Stage

from modules.aws import CloudWatch


class PrometheusMetricSender:
    GAUGE = "gauge"
    COUNTER = "counter"

    METRIC_CLASSES = {
        GAUGE: Gauge,
        COUNTER: Counter
    }

    def __init__(self, metrics=None, label_keys=None):
        self.registry = CollectorRegistry()

        self.metric_list = metrics
        self.label_keys = label_keys
        self.labels = {}

        self._metrics = self._create_metrics()

    def _create_metrics(self):
        return {
            metric_name: self.METRIC_CLASSES.get(metric_type)(
                metric_name, description, self.label_keys, registry=self.registry)
            for metric_name, (description, metric_type) in self.metric_list.items()
        }

    def set_labels(self, labels):
        self.labels = labels

    def submit(self, metric_name, value, labels=None):
        self._metrics[metric_name].labels(**{**self.labels, **(labels or {})}).set(value)

    def start(self):
        start_http_server(9156, registry=self.registry)

    def reset(self):
        for metric in self._metrics.values():
            self.registry.unregister(metric)
        self._metrics = self._create_metrics()


class MetricMonitor(SBOSCComponent):
    def __init__(self):
        super().__init__()
        self.metric_sender = PrometheusMetricSender(metrics={
            "sb_osc_worker_batch_size": ("Batch size of worker", PrometheusMetricSender.GAUGE),
            "sb_osc_worker_thread_count": ("Thread count of worker", PrometheusMetricSender.GAUGE),
            "sb_osc_worker_commit_interval": ("Commit interval of worker", PrometheusMetricSender.GAUGE),
            "sb_osc_last_event_timestamp": ("Timestamp of last event read from binlog", PrometheusMetricSender.GAUGE),
            "sb_osc_last_loaded_timestamp":
                ("Timestamp of last loaded event by EventLoader", PrometheusMetricSender.GAUGE),
            "sb_osc_last_catchup_timestamp":
                ("Last timestamp to reach the end of binlog", PrometheusMetricSender.GAUGE),
            "sb_osc_remaining_binlog_size": ("Remaining binlog size", PrometheusMetricSender.GAUGE),
            "sb_osc_average_insert_rate": ("Number of rows inserted per second", PrometheusMetricSender.GAUGE),
            "sb_osc_bulk_import_progress": ("Progress of bulk import", PrometheusMetricSender.GAUGE),
            "sb_osc_updated_pk_set_length": ("Length of updated PK set", PrometheusMetricSender.GAUGE),
            "sb_osc_removed_pk_set_length": ("Length of removed PK set", PrometheusMetricSender.GAUGE),
            "sb_osc_unmatched_rows": ("Number of unmatched row count", PrometheusMetricSender.GAUGE),
        }, label_keys=["dbclusteridentifier", "migration_id"])
        self.metric_sender.set_labels(labels={
            "dbclusteridentifier": config.SOURCE_CLUSTER_ID,
            "migration_id": self.migration_id
        })

        # Worker config
        self.previous_batch_size = self.redis_data.worker_config.batch_size
        self.previous_thread_count = self.redis_data.worker_config.thread_count
        self.optimal_batch_size = None
        self.optimal_thread_count = None
        self.optimal_value_use_count = 0

        # Worker metric
        self.previous_insert_rate = 0

        # CloudWatch
        self.cw = CloudWatch()

    def start(self):
        self.logger.info("Metric monitor started")
        self.metric_sender.start()
        while not self.stop_flag:
            current_stage = self.redis_data.current_stage
            if current_stage < Stage.DONE:
                if current_stage == Stage.BULK_IMPORT or Stage.APPLY_DML_EVENTS <= current_stage:
                    self.update_worker_config()
                self.check_migration_status()

            time.sleep(60)

        self.logger.info("Metric monitor stopped")

    def get_writer_cpu(self, dest_writer_id):
        datapoints = self.cw.get_instance_cpu_usages(
            instance_id=dest_writer_id,
            start_time=datetime.utcnow() - timedelta(minutes=3),
            end_time=datetime.utcnow(),
            statistics="Maximum"
        )
        if datapoints:
            writer_cpu = sum([datapoint['Maximum'] for datapoint in datapoints]) / len(datapoints)
        else:
            with self.db.cursor(DictCursor, host='dest') as cursor:
                cursor: DictCursor
                cursor.execute('''
                   SELECT CPU AS writer_cpu FROM information_schema.REPLICA_HOST_STATUS
                   WHERE SESSION_ID = 'MASTER_SESSION_ID'
               ''')
                writer_cpu = cursor.fetchone()['writer_cpu']
        return writer_cpu

    def get_write_latency(self, dest_writer_id):
        current_datetime = datetime.utcnow()
        datapoints = self.cw.get_rds_instance_metrics(
            instance_id=dest_writer_id,
            start_time=current_datetime - timedelta(minutes=5),
            end_time=current_datetime,
            metric_name="WriteLatency",
            statistics="Average",
            unit="Seconds",
        )['Datapoints']
        if datapoints:
            return sum([datapoint['Average'] * 1000 for datapoint in datapoints]) / len(datapoints)
        else:
            self.logger.warning("No datapoints for WriteLatency")
            return 0

    def get_optimal_worker_config(self, current_batch_size, current_thread_count, average_insert_rate) \
            -> Tuple[int, int]:
        # Return optimal values if they are already calculated
        if self.optimal_batch_size is not None and self.optimal_thread_count is not None:
            next_batch_size = self.optimal_batch_size
            next_thread_count = min(
                current_thread_count + config.THREAD_COUNT_STEP_SIZE,
                self.optimal_thread_count
            )

            # Reset optimal values if they are used for a while
            self.optimal_value_use_count += 1
            if self.optimal_value_use_count >= config.OPTIMAL_VALUE_USE_LIMIT:
                next_batch_size = current_batch_size + config.BATCH_SIZE_STEP_SIZE
                self.optimal_batch_size = None
                self.optimal_thread_count = None
                self.optimal_value_use_count = 0
        else:
            if self.previous_insert_rate == 0:
                next_batch_size = current_batch_size + config.BATCH_SIZE_STEP_SIZE
                next_thread_count = current_thread_count

            elif current_batch_size > self.previous_batch_size:
                # Increase batch size until insert rate stops increasing
                if average_insert_rate > self.previous_insert_rate:
                    next_batch_size = current_batch_size + config.BATCH_SIZE_STEP_SIZE
                    next_thread_count = current_thread_count
                else:
                    next_batch_size = self.previous_batch_size
                    next_thread_count = current_thread_count + config.THREAD_COUNT_STEP_SIZE
            elif current_thread_count > self.previous_thread_count or \
                    current_thread_count == config.MAX_THREAD_COUNT:
                # Increase thread count until insert rate stops increasing
                if average_insert_rate > self.previous_insert_rate:
                    next_batch_size = current_batch_size + config.BATCH_SIZE_STEP_SIZE
                    next_thread_count = current_thread_count
                else:
                    self.optimal_batch_size = current_batch_size
                    self.optimal_thread_count = self.previous_thread_count
                    next_batch_size = current_batch_size
                    next_thread_count = self.previous_thread_count
            elif current_batch_size == config.MAX_BATCH_SIZE:
                next_batch_size = current_batch_size
                next_thread_count = current_thread_count + config.THREAD_COUNT_STEP_SIZE

            else:
                next_batch_size = current_batch_size
                next_thread_count = current_thread_count

        # Save current values
        self.previous_insert_rate = average_insert_rate
        self.previous_batch_size = current_batch_size
        self.previous_thread_count = current_thread_count

        next_batch_size = min(next_batch_size, config.MAX_BATCH_SIZE)
        next_thread_count = min(next_thread_count, config.MAX_THREAD_COUNT)

        return next_batch_size, next_thread_count

    def submit_worker_metrics(
            self, current_batch_size, current_thread_count, commit_interval, average_insert_rate):
        self.metric_sender.submit('sb_osc_worker_batch_size', current_batch_size)
        self.metric_sender.submit('sb_osc_worker_thread_count', current_thread_count)
        self.metric_sender.submit('sb_osc_worker_commit_interval', commit_interval)
        self.metric_sender.submit('sb_osc_average_insert_rate', average_insert_rate)

    def update_worker_config(self):
        # Get writer metrics
        dest_writer_id = self.db.get_instance_id(host='dest', role='writer')
        writer_cpu = self.get_writer_cpu(dest_writer_id)
        write_latency = self.get_write_latency(dest_writer_id)
        self.logger.info(f"Writer CPU: {writer_cpu}%, Write Latency: {write_latency}ms")

        # Calculate average insert rate
        worker_metrics = self.redis_data.get_all_worker_metrics()
        if worker_metrics:
            average_insert_rate = sum([m.average_insert_rate for m in worker_metrics]) / len(worker_metrics)
        else:
            average_insert_rate = 0

        # Get current worker config
        worker_config = self.redis_data.worker_config
        next_batch_size = current_batch_size = worker_config.batch_size
        next_thread_count = current_thread_count = worker_config.thread_count
        commit_interval = worker_config.commit_interval

        if writer_cpu > config.CPU_HARD_THRESHOLD:
            self.logger.warning("CPU exceeded hard threshold. Setting thread count to 0.")
            next_batch_size = current_batch_size
            next_thread_count = 0

        elif writer_cpu > config.CPU_SOFT_THRESHOLD:
            self.logger.warning("CPU exceeded soft threshold. Start decreasing thread count.")
            next_batch_size = current_batch_size
            next_thread_count = max(config.MIN_THREAD_COUNT, current_thread_count // 2)

        elif write_latency > config.WRITE_LATENCY_HARD_THRESHOLD:
            self.logger.warning("Latency exceeded hard threshold. Setting thread count to 0.")
            next_batch_size = config.MIN_BATCH_SIZE
            next_thread_count = 0

        elif write_latency > config.WRITE_LATENCY_SOFT_THRESHOLD:
            self.logger.warning("Latency exceeded soft threshold. Start decreasing batch size.")
            next_batch_size = max(config.MIN_BATCH_SIZE, current_batch_size // 2)
            next_thread_count = current_thread_count

        elif writer_cpu < config.CPU_SOFT_THRESHOLD and \
                write_latency < config.WRITE_LATENCY_SOFT_THRESHOLD and current_thread_count == 0:
            self.logger.info("Writer became stable. Restoring thread count to MIN_THREAD_COUNT.")
            next_batch_size = current_batch_size
            next_thread_count = config.MIN_THREAD_COUNT

        elif average_insert_rate > 0 and self.redis_data.current_stage == Stage.BULK_IMPORT:
            next_batch_size, next_thread_count = self.get_optimal_worker_config(
                current_batch_size, current_thread_count, average_insert_rate * current_thread_count)

        self.submit_worker_metrics(
            current_batch_size, current_thread_count, commit_interval, average_insert_rate)
        self.logger.info("Submitted worker metrics")

        if next_batch_size != current_batch_size or next_thread_count != current_thread_count:
            self.redis_data.worker_config.set({
                'batch_size': next_batch_size,
                'thread_count': next_thread_count,
                'commit_interval': commit_interval,
                'revision': worker_config.revision + 1
            })

        # Unlike batch_size, thread_count requires some time to take effect
        if next_thread_count != current_thread_count:
            time.sleep(60)

    def submit_event_handler_timestamps(self):
        # last_event_timestamp, last_loaded_timestamp, last_catchup_timestamp
        with self.db.cursor() as cursor:
            cursor: Cursor
            cursor.execute(f'''
                SELECT MIN(last_event_timestamp), MAX(last_event_timestamp)
                FROM {config.SBOSC_DB}.event_handler_status WHERE migration_id = %s AND last_event_timestamp > 1
            ''' % self.migration_id)
            start_timestamp, last_timestamp = cursor.fetchone()
            if start_timestamp:
                last_event_timestamp = last_timestamp - start_timestamp
                self.metric_sender.submit('sb_osc_last_event_timestamp', last_event_timestamp)

                cursor.execute(f'''
                    SELECT last_loaded_timestamp FROM {config.SBOSC_DB}.apply_dml_events_status
                    WHERE migration_id = %s ORDER BY id DESC LIMIT 1
                ''' % self.migration_id)
                if cursor.rowcount > 0:
                    last_loaded_timestamp = cursor.fetchone()[0] - start_timestamp
                    self.metric_sender.submit('sb_osc_last_loaded_timestamp', last_loaded_timestamp)

                last_catchup_timestamp = self.redis_data.last_catchup_timestamp - start_timestamp
                if last_catchup_timestamp > 0:
                    self.metric_sender.submit('sb_osc_last_catchup_timestamp', last_catchup_timestamp)

    def check_migration_status(self):
        # bulk_import_progress
        inserted_rows = 0
        for chunk_id in self.redis_data.chunk_set:
            chunk_info = self.redis_data.get_chunk_info(chunk_id)
            last_pk_inserted = chunk_info.last_pk_inserted

            # last_pk_inserted is initialized as batch_start_pk - 1
            if last_pk_inserted and last_pk_inserted >= chunk_info.start_pk:
                inserted_rows += last_pk_inserted - chunk_info.start_pk

        if self.redis_data.metadata.max_pk:
            bulk_import_progress = inserted_rows / self.redis_data.metadata.max_pk * 100
            self.metric_sender.submit('sb_osc_bulk_import_progress', bulk_import_progress)

        self.submit_event_handler_timestamps()

        # remaining_binlog_size
        remaining_binlog_size = 0
        if time.time() - self.redis_data.last_catchup_timestamp > 2:
            with self.db.cursor() as cursor:
                cursor.execute(f'''
                    SELECT log_file, log_pos FROM {config.SBOSC_DB}.event_handler_status
                    WHERE migration_id = %s ORDER BY id DESC LIMIT 1
                ''' % self.migration_id)
                if cursor.rowcount > 0:
                    log_file, log_pos = cursor.fetchone()
                    remaining_binlog_size = 0
                    cursor.execute("SHOW BINARY LOGS")
                    for log_name, file_size, *_ in cursor.fetchall():  # *_ if for MySQL 5.7 and 8.0 compatibility
                        if log_name >= log_file:
                            remaining_binlog_size += file_size
                    remaining_binlog_size -= log_pos
        self.metric_sender.submit('sb_osc_remaining_binlog_size', remaining_binlog_size)

        # updated_pk_set, removed_pk_set
        updated_pk_set_len = len(self.redis_data.updated_pk_set)
        removed_pk_set_len = len(self.redis_data.removed_pk_set)
        self.metric_sender.submit('sb_osc_updated_pk_set_length', updated_pk_set_len)
        self.metric_sender.submit('sb_osc_removed_pk_set_length', removed_pk_set_len)

        # unmatched_pks
        with self.db.cursor() as cursor:
            cursor.execute(
                f"SELECT COUNT(1) FROM {config.SBOSC_DB}.unmatched_rows WHERE migration_id = %s" % self.migration_id)
            unmatched_pks = cursor.fetchone()[0]
            self.metric_sender.submit('sb_osc_unmatched_rows', unmatched_pks)
