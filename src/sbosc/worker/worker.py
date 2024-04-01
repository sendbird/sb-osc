import time
from collections import deque
from typing import Callable, Self

import MySQLdb
from MySQLdb.cursors import Cursor

from typing import TYPE_CHECKING

from config import config
from modules.db import Database
from modules.redis import RedisData
from modules.redis.schema import ChunkInfo
from sbosc.const import WorkerStatus, Stage, ChunkStatus
from sbosc.operations.operation import MigrationOperation

if TYPE_CHECKING:
    from sbosc.worker.manager import WorkerManager


class WorkerConfig:
    def __init__(self, redis_data: RedisData, datapoints: deque):
        self.redis_data = redis_data
        self.datapoints = datapoints
        self.worker_config_revision = -1  # default revision is 0
        self.batch_size_multiplier = 1

        # config
        self.raw_batch_size = None
        self.batch_size = None
        self.commit_interval = None

    def update(self):
        worker_config = self.redis_data.worker_config
        if worker_config.revision > self.worker_config_revision:
            self.raw_batch_size = worker_config.batch_size
            self.commit_interval = worker_config.commit_interval
            self.worker_config_revision = worker_config.revision
            self.datapoints.clear()
        self.batch_size = int(self.raw_batch_size * self.batch_size_multiplier)

    def update_batch_size_multiplier(self, rowcount):
        if rowcount == 0:
            self.batch_size_multiplier += 1
        elif self.raw_batch_size == rowcount:
            self.batch_size_multiplier = \
                max(1.0, self.batch_size_multiplier * 0.9)
        else:
            self.batch_size_multiplier = self.batch_size / rowcount


class Worker:
    def __init__(self, worker_id, manager: 'WorkerManager'):
        self.migration_id = manager.migration_id
        self.db = Database()
        self.redis_data = RedisData(self.migration_id)
        self.logger = manager.logger
        self.migration_operation: MigrationOperation = config.OPERATION_CLASS(self.migration_id)
        self.use_batch_size_multiplier = config.USE_BATCH_SIZE_MULTIPLIER
        self.worker_id = worker_id
        self.stop_flag = False
        self.status = WorkerStatus.IDLE
        self.datapoints = deque(maxlen=100)
        self.worker_config = WorkerConfig(self.redis_data, self.datapoints)
        self.interval = 60

        self.old_source_table = None

    def set_stop_flag(self):
        self.stop_flag = True

    def start(self):
        self.logger.info(f"Worker {self.worker_id} started")
        while not self.stop_flag:
            self.status = WorkerStatus.IDLE
            if self.redis_data.current_stage == Stage.BULK_IMPORT:
                self.bulk_import()
                self.interval = 1
            # For apply DML events, it keeps running until the end of the process
            elif Stage.APPLY_DML_EVENTS <= self.redis_data.current_stage < Stage.DONE:
                if self.redis_data.current_stage == Stage.SWAP_TABLES:
                    self.interval = 0.1
                    self.old_source_table = self.redis_data.old_source_table
                else:
                    self.interval = 1
                self.apply_dml_events()
            else:
                self.interval = 60

            time.sleep(self.interval)

        self.logger.info(f"Worker {self.worker_id} stopped")

    def get_start_pk(self, chunk_info: ChunkInfo):
        if chunk_info.status == ChunkStatus.DONE:
            return None
        elif chunk_info.status == ChunkStatus.NOT_STARTED:
            chunk_info.last_pk_inserted = chunk_info.start_pk - 1
            return chunk_info.start_pk
        elif chunk_info.status == ChunkStatus.IN_PROGRESS:
            return chunk_info.last_pk_inserted + 1
        elif chunk_info.status == ChunkStatus.DUPLICATE_KEY:
            max_pk = self.get_max_pk(chunk_info.start_pk, chunk_info.end_pk)
            return max_pk + 1

    def bulk_import(self):
        chunk_id = self.redis_data.chunk_stack.pop()
        if chunk_id is None:
            return

        try:
            self.logger.info(f"Worker {self.worker_id} started processing chunk {chunk_id}")
            self.status = WorkerStatus.BUSY

            chunk_info = self.redis_data.get_chunk_info(chunk_id)
            start_pk = self.get_start_pk(chunk_info)
            if start_pk is None:
                return

            end_pk = chunk_info.end_pk
            chunk_info.status = ChunkStatus.IN_PROGRESS

            self.worker_config.update()
            batch_start_pk = start_pk
            batch_end_pk = min(batch_start_pk + self.worker_config.batch_size - 1, end_pk)

            while batch_start_pk <= end_pk and not self.stop_flag:
                try:
                    cursor = self.insert_batch(batch_start_pk, batch_end_pk)
                except MySQLdb.IntegrityError as e:
                    # Retry on duplicate key error
                    self.logger.error(f"Integrity error: {e}. Retrying with upsert query.")
                    self.insert_batch(batch_start_pk, batch_end_pk, upsert=True)
                    # Set chunk status to DUPLICATE_KEY
                    chunk_info.status = ChunkStatus.DUPLICATE_KEY
                    break

                # update batch size multiplier
                if self.use_batch_size_multiplier:
                    self.worker_config.update_batch_size_multiplier(cursor.rowcount)

                # update last pk inserted
                if cursor.rowcount == self.worker_config.raw_batch_size:
                    print('last row id', cursor.lastrowid)
                    last_pk_inserted = cursor.lastrowid
                else:
                    last_pk_inserted = batch_end_pk
                self.logger.info(
                    f"Worker {self.worker_id} finished processing batch "
                    f"{batch_start_pk} - {last_pk_inserted}. "
                    f"Batch size multiplier: {self.worker_config.batch_size_multiplier}"
                )
                chunk_info.last_pk_inserted = last_pk_inserted

                # Get next batch
                self.worker_config.update()
                batch_start_pk = last_pk_inserted + 1
                batch_end_pk = min(batch_start_pk + self.worker_config.batch_size - 1, end_pk)

        except Exception as e:
            self.logger.error(e)

        # This part will be executed when the worker is stopped or the chunk is done or exception is raised
        chunk_info = self.redis_data.get_chunk_info(chunk_id)
        if chunk_info.last_pk_inserted == chunk_info.end_pk:
            chunk_info.status = ChunkStatus.DONE
            self.logger.info(f"Worker {self.worker_id} finished processing chunk {chunk_id}")
        else:
            self.redis_data.push_chunk(chunk_id)
            self.logger.warning(
                f"Worker {self.worker_id} stopped processing chunk {chunk_id} at {chunk_info.last_pk_inserted}")
        self.worker_config.batch_size_multiplier = 1

    def apply_dml_events(self):
        try:
            updated_pk_set = self.redis_data.updated_pk_set
            removed_pk_set = self.redis_data.removed_pk_set
            while (len(updated_pk_set) > 0 or len(removed_pk_set) > 0) and not self.stop_flag:
                self.status = WorkerStatus.BUSY

                self.worker_config.update()
                batch_size = self.worker_config.batch_size
                if len(updated_pk_set) > len(removed_pk_set):
                    updated_pks = updated_pk_set.get(batch_size)
                    if updated_pks:
                        self.logger.info(
                            f"Worker {self.worker_id} started processing {len(updated_pks)} updated events. "
                            f"Updated pks: {updated_pks}"
                        )
                        self.apply_update(updated_pks)
                        self.logger.info(
                            f"Worker {self.worker_id} finished processing {len(updated_pks)} updated events")
                else:
                    removed_pks = removed_pk_set.get(batch_size)
                    if removed_pks:
                        self.logger.info(
                            f"Worker {self.worker_id} started processing {len(removed_pks)} removed events. "
                            f"Removed pks: {removed_pks}")
                        self.apply_delete(removed_pks)
                        self.logger.info(
                            f"Worker {self.worker_id} finished processing {len(removed_pks)} removed events")

        except Exception as e:
            self.logger.error(e)

    def get_max_pk(self, start_pk, end_pk):
        metadata = self.redis_data.metadata
        with self.db.cursor(host='dest') as cursor:
            cursor: Cursor
            cursor.execute(f'''
                SELECT MAX(id) FROM {metadata.destination_db}.{metadata.destination_table}
                WHERE id BETWEEN {start_pk} AND {end_pk}
            ''')
            return cursor.fetchone()[0]

    @staticmethod
    def calculate_metrics(func: Callable[..., Cursor]):
        def wrapper(self: Self, *args, **kwargs):
            t1 = time.time()
            cursor = func(self, *args, **kwargs)
            time.sleep(self.worker_config.commit_interval)
            t2 = time.time()

            insert_rate = cursor.rowcount / (t2 - t1)  # rows per second
            self.datapoints.append(insert_rate)
            return cursor

        return wrapper

    @calculate_metrics
    def insert_batch(self, batch_start_pk, batch_end_pk, upsert=False):
        limit = self.worker_config.raw_batch_size if self.use_batch_size_multiplier else None
        return self.migration_operation.insert_batch(self.db, batch_start_pk, batch_end_pk, upsert, limit)

    @calculate_metrics
    def apply_update(self, updated_pks):
        with self.migration_operation.override_source_table(self.old_source_table):
            return self.migration_operation.apply_update(self.db, updated_pks)

    @calculate_metrics
    def apply_delete(self, removed_pks):
        with self.db.cursor(host="dest") as cursor:
            metadata = self.redis_data.metadata
            removed_pks_str = ",".join([str(pk) for pk in removed_pks])
            query = f"""
                DELETE FROM {metadata.destination_db}.{metadata.destination_table}
                WHERE id IN ({removed_pks_str})
            """
            cursor.execute(query)
        return cursor
