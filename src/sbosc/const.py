class Stage:
    START_EVENT_HANDLER = '01_start_event_handler'
    BULK_IMPORT_CHUNK_CREATION = '02_bulk_import_chunk_creation'
    BULK_IMPORT = '03_bulk_import'
    BULK_IMPORT_VALIDATION = '04_bulk_import_validation'
    BULK_IMPORT_VALIDATION_FAILED = '04_1_bulk_import_validation_failed'
    APPLY_DML_EVENTS = '05_apply_dml_events'
    APPLY_DML_EVENTS_PRE_VALIDATION = '06_apply_dml_events_pre_validation'
    ADD_INDEX = '06_1_add_index'
    APPLY_DML_EVENTS_VALIDATION = '07_apply_dml_events_validation'
    SWAP_TABLES = '08_swap_tables'
    SWAP_TABLES_FAILED = '08_1_swap_tables_failed'
    DONE = '09_done'


class ChunkStatus:
    NOT_STARTED = 'not_started'
    IN_PROGRESS = 'in_progress'
    DUPLICATE_KEY = 'duplicate_key'
    DONE = 'done'


class WorkerStatus:
    IDLE = 'idle'
    BUSY = 'busy'


class UnmatchType:
    NOT_UPDATED = 'not_updated'
    NOT_REMOVED = 'not_removed'
