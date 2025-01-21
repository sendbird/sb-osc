import concurrent.futures
import time
from threading import Thread

from MySQLdb.cursors import Cursor, DictCursor
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent, RowsEvent

from config import config, secret
from modules.slack import SlackClient
from sbosc.component import SBOSCComponent
from sbosc.const import Stage
from sbosc.eventhandler.eventloader import EventLoader


class EventStore:
    def __init__(self):
        self.handled_events = 0
        self.holding_events = 0
        self.last_event_timestamp = 1
        self.insert_event_timestamp = {}
        self.update_event_timestamp = {}
        self.delete_event_timestamp = {}

    @staticmethod
    def parse_dml_event(event: RowsEvent):
        # UpdateRowsEvent has 'before_values' and 'after_values' attributes, while others have 'values'
        affected_pks = [list(row.values())[0][event.primary_key] for row in event.rows]
        timestamp = event.timestamp
        return affected_pks, timestamp

    def add_event(self, event: RowsEvent):
        event_timestamp_dict = None

        affected_pks, timestamp = self.parse_dml_event(event)
        if isinstance(event, WriteRowsEvent):
            event_timestamp_dict = self.insert_event_timestamp
        elif isinstance(event, UpdateRowsEvent):
            event_timestamp_dict = self.update_event_timestamp
        elif isinstance(event, DeleteRowsEvent):
            event_timestamp_dict = self.delete_event_timestamp

        if event_timestamp_dict is not None:
            for pk in affected_pks:
                event_timestamp_dict[pk] = timestamp

        if self.last_event_timestamp < timestamp:
            self.last_event_timestamp = timestamp

        self.handled_events += 1
        self.holding_events += 1

    def merge(self, event_store):
        self.insert_event_timestamp.update(event_store.insert_event_timestamp)
        self.update_event_timestamp.update(event_store.update_event_timestamp)
        self.delete_event_timestamp.update(event_store.delete_event_timestamp)
        self.last_event_timestamp = max(self.last_event_timestamp, event_store.last_event_timestamp)
        self.handled_events += event_store.handled_events
        self.holding_events += event_store.holding_events

    def clear(self):
        self.holding_events = 0
        self.insert_event_timestamp = {}
        self.update_event_timestamp = {}
        self.delete_event_timestamp = {}


class EventHandler(SBOSCComponent):
    def __init__(self):
        super().__init__()
        self.thread_count = config.EVENTHANDLER_THREAD_COUNT
        self.thread_timeout = config.EVENTHANDLER_THREAD_TIMEOUT_IN_SECONDS
        self.slack = SlackClient('SB-OSC EventHandler', f'{config.SOURCE_CLUSTER_ID}, {self.migration_id}')

        # EventLoader
        self.event_loader = EventLoader(self)
        self.event_loader_thread = None

        # BinlogStreamReader
        self.connection_settings = {
            'host': config.SOURCE_WRITER_ENDPOINT,
            'port': secret.PORT,
            'user': secret.USERNAME,
            'passwd': secret.PASSWORD,
        }

        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=self.thread_count)

        self.log_file = None
        self.log_pos = None
        self.live_mode = False

        self.event_store = EventStore()
        self.last_saved_timestamp = 1
        self.handled_binlog_files = set()

    def set_stop_flag(self):
        self.logger.info('Stopping event handler...')
        self.stop_flag = True
        self.event_loader.set_stop_flag()

    def init_event_handler(self):
        with self.db.cursor(DictCursor) as cursor:
            cursor: DictCursor
            cursor.execute(f'''
                SELECT log_file, log_pos, last_event_timestamp, created_at FROM {config.SBOSC_DB}.event_handler_status
                WHERE migration_id = {self.migration_id} ORDER BY id DESC LIMIT 1
            ''')

            if cursor.rowcount > 0:
                status = cursor.fetchone()
                self.log_file = status['log_file']
                self.log_pos = status['log_pos']
                self.event_store.last_event_timestamp = status['last_event_timestamp']
                self.last_saved_timestamp = status['created_at'].timestamp()
                self.logger.info(f'Resuming from binlog position: {self.log_file} {self.log_pos}')
            else:
                if config.INIT_BINLOG_FILE:
                    self.log_file = config.INIT_BINLOG_FILE
                    self.log_pos = config.INIT_BINLOG_POSITION
                    self.last_saved_timestamp = int(time.time())
                else:
                    cursor.execute("SHOW MASTER STATUS")
                    status = cursor.fetchone()
                    self.log_file = status['File']
                    self.log_pos = status['Position']
                    self.last_saved_timestamp = self.event_store.last_event_timestamp = int(time.time())
                self.save_current_binlog_position()
                self.slack.send_message(
                    subtitle="EventHandler started binlog stream",
                    message=f"Log file: {self.log_file}\n"
                            f"Log position: {self.log_pos}"
                )

        if self.redis_data.current_stage == Stage.START_EVENT_HANDLER:
            if config.SKIP_BULK_IMPORT:
                self.redis_data.set_current_stage(Stage.APPLY_DML_EVENTS)
            else:
                self.redis_data.set_current_stage(Stage.BULK_IMPORT_CHUNK_CREATION)

    def create_binlog_stream(self, log_file, log_pos, thread_id=0) -> BinLogStreamReader:
        metadata = self.redis_data.metadata
        return BinLogStreamReader(
            connection_settings=self.connection_settings,
            server_id=int(self.migration_id) * 100 + thread_id,
            resume_stream=True,
            only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent],
            only_schemas=[metadata.source_db],
            only_tables=[metadata.source_table],
            log_file=log_file,
            log_pos=log_pos
        )

    def save_current_binlog_position(self):
        with self.db.cursor() as cursor:
            cursor: Cursor
            cursor.execute(f'''
                INSERT INTO {config.SBOSC_DB}.event_handler_status
                (migration_id, log_file, log_pos, last_event_timestamp, created_at)
                VALUES (%s, %s, %s, %s, NOW())
            ''', (self.migration_id, self.log_file, self.log_pos, self.event_store.last_event_timestamp))
        self.logger.info(f'Saved binlog position: {self.log_file} {self.log_pos}')

    def save_events_to_db(self):
        with self.db.cursor() as cursor:
            cursor: Cursor
            for table_name, events in [
                (f'inserted_pk_{self.migration_id}', self.event_store.insert_event_timestamp.items()),
                (f'updated_pk_{self.migration_id}', self.event_store.update_event_timestamp.items()),
                (f'deleted_pk_{self.migration_id}', self.event_store.delete_event_timestamp.items())
            ]:
                cursor.executemany(f'''
                    INSERT INTO {config.SBOSC_DB}.{table_name} (source_pk, event_timestamp)
                    VALUES (%s, %s) ON DUPLICATE KEY UPDATE event_timestamp = VALUES(event_timestamp)
                ''', list(events))
        self.event_store.clear()
        self.logger.info('Saved events to db')

    def save(self):
        self.save_events_to_db()
        self.save_current_binlog_position()
        self.logger.info(f'Handled binlog files: {self.handled_binlog_files}')
        self.handled_binlog_files.clear()
        self.last_saved_timestamp = time.time()

    def start(self):
        self.logger.info('Starting event handler')
        while not self.stop_flag:
            current_stage = self.redis_data.current_stage
            if Stage.DONE > current_stage >= Stage.START_EVENT_HANDLER and not config.DISABLE_EVENTHANDLER:
                if self.log_file is None or self.log_pos is None:
                    self.logger.info('Initializing event handler')
                    self.init_event_handler()
                else:
                    if current_stage == Stage.APPLY_DML_EVENTS and not config.DISABLE_APPLY_DML_EVENTS:
                        self.apply_dml_events()
                    elif current_stage == Stage.APPLY_DML_EVENTS_PRE_VALIDATION:
                        self.apply_dml_events_pre_validation()
                    elif current_stage >= Stage.APPLY_DML_EVENTS_VALIDATION:
                        self.live_mode = True
                    self.follow_event_stream()
                time.sleep(0.1)
                if self.redis_data.current_stage == Stage.START_EVENT_HANDLER:
                    self.redis_data.set_current_stage(Stage.BULK_IMPORT_CHUNK_CREATION)
            else:
                time.sleep(60)

        # Save events and binlog position before exiting
        self.save()
        self.logger.info('Saved events and binlog position before exiting.')

    def are_indexes_created(self):
        with self.db.cursor() as cursor:
            cursor: Cursor
            cursor.execute(f'''
                SELECT COUNT(1) FROM {config.SBOSC_DB}.index_creation_status
                WHERE migration_id = {self.migration_id} AND ended_at IS NULL
            ''')
            return cursor.fetchone()[0] == 0

    def start_event_loader(self):
        if self.event_loader_thread is None or not self.event_loader_thread.is_alive():
            if self.event_loader_thread is not None:
                self.logger.warning('Event loader thread is dead, restarting...')
                self.event_loader = EventLoader(self)

            self.event_loader_thread = Thread(target=self.event_loader.start)
            self.event_loader_thread.start()
            time.sleep(60)

    def apply_dml_events(self):
        self.start_event_loader()
        if len(self.redis_data.updated_pk_set) == 0 and len(self.redis_data.removed_pk_set) == 0 and \
                self.event_store.last_event_timestamp - self.event_loader.last_loaded_timestamp < 60:
            self.redis_data.set_current_stage(Stage.APPLY_DML_EVENTS_PRE_VALIDATION)

    def apply_dml_events_pre_validation(self):
        self.start_event_loader()
        self.save()
        with self.db.cursor() as cursor:
            cursor: Cursor
            cursor.execute(f"SELECT COUNT(1) FROM {config.SBOSC_DB}.inserted_pk_{self.migration_id}")
            inserted_count = cursor.fetchone()[0]
            cursor.execute(f"SELECT COUNT(1) FROM {config.SBOSC_DB}.updated_pk_{self.migration_id}")
            updated_count = cursor.fetchone()[0]
            cursor.execute(f"SELECT COUNT(1) FROM {config.SBOSC_DB}.deleted_pk_{self.migration_id}")
            deleted_count = cursor.fetchone()[0]
        if inserted_count + updated_count + deleted_count > 0:
            while self.event_store.last_event_timestamp != self.event_loader.last_loaded_timestamp:
                if self.stop_flag:
                    return
                time.sleep(60)
        self.event_loader.set_stop_flag()
        if self.are_indexes_created():
            self.live_mode = True
            self.redis_data.set_current_stage(Stage.APPLY_DML_EVENTS_VALIDATION)
        else:
            self.redis_data.set_current_stage(Stage.ADD_INDEX)

    @staticmethod
    def parse_binlog_batch(stream):
        event_store = EventStore()
        start_file = stream.log_file
        for event in stream:
            event_store.add_event(event)
            if stream.log_file != start_file:
                break
        end_file = stream.log_file
        end_pos = stream.log_pos
        stream.close()
        return event_store, (end_file, end_pos)

    def follow_event_stream(self):
        target_files = []

        # Create binlog batch queue
        with self.db.cursor(DictCursor) as cursor:
            cursor: DictCursor
            last_binlog_check_timestamp = time.time()
            cursor.execute("SHOW MASTER STATUS")
            live_log_file = cursor.fetchone()['File']
            live_index_number = live_log_file.split('.')[-1]
            base_name, index_number = self.log_file.split('.')
            binlog_files = [
                '{}.{:06d}'.format(base_name, i)
                for i in range(int(index_number), int(live_index_number) + 1)
            ]
            for log_file in binlog_files[:self.thread_count]:
                start_pos = self.log_pos if log_file == self.log_file else 4
                target_files.append((log_file, start_pos))

        # Parse binlog batches
        threads = []
        event_store = EventStore()
        result_event_stores = []
        done_files = []

        for thread_id in range(len(target_files)):
            binlog_file, start_pos = target_files[thread_id]
            stream = self.create_binlog_stream(binlog_file, start_pos, thread_id)
            threads.append(self.executor.submit(self.parse_binlog_batch, stream))
        done, not_done = concurrent.futures.wait(threads, timeout=self.thread_timeout)
        if len(not_done) > 0:
            self.set_stop_flag()
            raise Exception('Binlog batch parsing timed out')

        for thread in threads:
            result_event_store, done_file = thread.result()
            result_event_stores.append(result_event_store)
            done_files.append(done_file)

        if self.stop_flag:
            self.logger.info('Binlog parsing stopped')
        else:
            self.log_file, self.log_pos = max(done_files)
            self.handled_binlog_files = self.handled_binlog_files | set([binlog_file for binlog_file, _ in done_files])

            # Merge event stores
            for result_event_store in sorted(result_event_stores, key=lambda x: x.last_event_timestamp):
                event_store.merge(result_event_store)

            if event_store.handled_events > 0:
                if self.live_mode:
                    updated_pks = \
                        set(event_store.insert_event_timestamp.keys()) | set(event_store.update_event_timestamp.keys())
                    removed_pks = set(event_store.delete_event_timestamp.keys())
                    updated_pks = updated_pks - removed_pks
                    self.redis_data.updated_pk_set.add(updated_pks - removed_pks)
                    self.redis_data.updated_pk_set.remove(removed_pks)
                    self.redis_data.removed_pk_set.add(removed_pks)

                self.event_store.merge(event_store)
                self.logger.info(f'Handled {self.event_store.handled_events} events.')
                if self.event_store.holding_events >= 1000 or self.redis_data.current_stage == Stage.SWAP_TABLES:
                    # Save events and binlog position
                    self.save()

            elif time.time() - self.last_saved_timestamp > 600:
                self.save()

            if len(binlog_files) == 1:
                self.redis_data.set_last_catchup_timestamp(last_binlog_check_timestamp)
