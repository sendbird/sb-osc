# Validator

Validator is a component that validates data consistency between source and destination tables during migration.

## Validation Types

```
1. bulk_import_validation     - Validates bulk imported data
2. apply_dml_events_validation - Validates DML events applied by Worker
3. full_dml_event_validation   - Periodic full validation of all DML events
```

---

## Bulk Import Validation

### Purpose

- Validates data imported during bulk import phase
- Ensures all rows from source table exist in destination table
- PK-based validation (not timestamp-based)
- Processes in chunks for checkpointing and resumability

### Config Parameters

```python
BULK_IMPORT_VALIDATION_BATCH_SIZE = 100000   # Batch size for multi-threaded validation
BULK_IMPORT_VALIDATION_CHUNK_SIZE = 10000000 # Chunk size for checkpointing
```

### Validation Flow

```
1. Get last checkpoint (last_validated_pk)
2. Process PK range [last_validated_pk + 1, max_pk] in chunks
3. Within each chunk, multi-threaded batch validation
4. Save checkpoint after each chunk
5. If validation fails, return False immediately
```

### Checkpoint & Resume Logic

```python
# Get last checkpoint
SELECT chunk_end_pk FROM bulk_import_validation_status
WHERE migration_id = ? AND is_valid = TRUE
ORDER BY id DESC LIMIT 1

# Resume from checkpoint
chunk_start_pk = last_validated_pk + 1 if last_validated_pk > 0 else 0
```

### Chunk Processing

```python
while chunk_start_pk <= max_pk:
    chunk_end_pk = min(chunk_start_pk + chunk_size - 1, max_pk)

    # Create batches within chunk
    range_queue = Queue()
    batch_start = chunk_start_pk
    while batch_start <= chunk_end_pk:
        range_queue.put((batch_start, min(batch_start + batch_size, chunk_end_pk)))
        batch_start += batch_size + 1

    # Multi-threaded validation
    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        threads = [executor.submit(__validate_bulk_import_batch, range_queue, failed_pks) for _ in range(thread_count)]
        is_chunk_valid = all([thread.result() for thread in threads])

    # Save checkpoint
    INSERT INTO bulk_import_validation_status
    (migration_id, chunk_end_pk, is_valid, created_at)
    VALUES (?, chunk_end_pk, is_chunk_valid, NOW())

    if not is_chunk_valid:
        return False  # Stop on failure

    chunk_start_pk = chunk_end_pk + 1
```

### Failure Handling

**Bulk import validation failure is critical and unrecoverable.**

If source has a row that doesn't exist in destination, it cannot be recovered in later stages (DML events only capture changes after bulk import).

On failure:
1. Critical log: `"Failed to validate bulk import. Failed pks: {failed_pks}"`
2. Stage transitions to `BULK_IMPORT_VALIDATION_FAILED`
3. Slack notification: "Bulk import validation failed"
4. Migration stops - manual intervention required

---

## Apply DML Events Validation

### Basic Operation

```
Worker applies DML events
    │
    ▼
apply_dml_events_validation() called
    │
    ▼
Compares source vs destination for each event type
    │
    ▼
Records unmatched rows to unmatched_rows table
    │
    ▼
Saves result to apply_dml_events_validation_status
```

### Core Functions

1. **`__get_timestamp_range()`**: Gets start/end timestamp for validation
   - `start_timestamp`: from `apply_dml_events_validation_status.last_validated_timestamp` or min event timestamp
   - `end_timestamp`: from `event_handler_status.last_event_timestamp`

2. **`validate_apply_dml_events(start, end)`**: Main validation logic
   - Validates `inserted_pk`, `updated_pk`, `deleted_pk` tables
   - Multi-threaded validation within timestamp ranges
   - Records unmatched rows

3. **`__validate_unmatched_pks()`**: Re-validates previously unmatched PKs
   - Some unmatched rows may have been fixed by subsequent DML events

### Validation Flow

```python
def apply_dml_events_validation(self):
    start_timestamp, end_timestamp = self.__get_timestamp_range()

    is_valid = self.validate_apply_dml_events(start_timestamp, end_timestamp)

    # Save result
    INSERT INTO apply_dml_events_validation_status
    (migration_id, last_validated_timestamp, is_valid, created_at)
    VALUES (...)
```

---

## Full DML Event Validation

### Purpose

- Periodically validates ALL DML events from the beginning
- Runs every `FULL_DML_EVENT_VALIDATION_INTERVAL_IN_HOURS` (default: 1 hour)
- Processes in chunks for checkpointing and resumability

### Config Parameters

```python
FULL_DML_EVENT_VALIDATION_INTERVAL_IN_HOURS = 1    # Validation interval
FULL_DML_EVENT_VALIDATION_CHUNK_DURATION_IN_HOURS = 1  # Chunk size (hours)
```

### Checkpoint Table

```sql
CREATE TABLE full_dml_event_validation_status (
    id int PRIMARY KEY AUTO_INCREMENT,
    migration_id int,
    target_end_timestamp bigint,      -- Target end timestamp for this validation run
    last_validated_timestamp bigint,  -- Last validated chunk end timestamp
    is_valid bool,
    created_at datetime
);
```

### Validation Flow

```
1. Check interval (skip if < interval since last validation)
2. Check if previous validation is unfinished (resume if so)
3. Get start_timestamp (min event timestamp) and end_timestamp (last_event_timestamp)
4. Process in chunks (chunk_duration seconds each)
5. Save checkpoint after each chunk
```

### Checkpoint & Resume Logic

```python
# Get last checkpoint
SELECT last_validated_timestamp, target_end_timestamp
FROM full_dml_event_validation_status
WHERE migration_id = ? ORDER BY id DESC LIMIT 1

# Case 1: Within interval → Skip
if datetime.now() - last_validated_timestamp < interval:
    return False  # Skip

# Case 2: Unfinished validation → Resume
if last_validated_timestamp < target_end_timestamp:
    start_timestamp = last_validated_timestamp + 1
    end_timestamp = target_end_timestamp

# Case 3: New validation → Fresh start
else:
    start_timestamp = MIN(event_timestamp) from all event tables
    end_timestamp = last_event_timestamp from event_handler_status
```

### Chunk Processing

```python
target_end_timestamp = end_timestamp
chunk_start_timestamp = start_timestamp

while chunk_start_timestamp <= target_end_timestamp:
    chunk_end_timestamp = min(
        chunk_start_timestamp + chunk_duration,
        target_end_timestamp
    )

    is_valid = self.validate_apply_dml_events(chunk_start_timestamp, chunk_end_timestamp)

    # Save checkpoint
    INSERT INTO full_dml_event_validation_status
    (migration_id, target_end_timestamp, last_validated_timestamp, is_valid, created_at)
    VALUES (?, target_end_timestamp, chunk_end_timestamp, is_valid, NOW())

    chunk_start_timestamp = chunk_end_timestamp + 1
```

---

## DML Event Validation: Eventually Consistent

**Unlike bulk import validation, DML event validation is eventually consistent.**

### Why is_valid = False is OK

DML event validation (`apply_dml_events_validation` and `full_dml_event_validation`) may temporarily return `is_valid = False` due to:

1. **Timing gap**: Validation runs while Worker is still applying events
2. **Duplicate updates**: Same row updated multiple times in quick succession
3. **Race conditions**: Event captured but not yet applied

These are expected and will resolve themselves as Worker continues processing.

### How Unmatched Rows are Resolved

```
1. Validation finds unmatched rows → Records to unmatched_rows table
2. __validate_unmatched_pks() re-checks these rows
3. If row now matches → Removed from unmatched_rows
4. If still unmatched → Pushed to Redis for Worker to retry
5. Repeat until unmatched_rows is empty
```

### When is_valid Actually Matters

**Only the final validation before table swap is meaningful.**

In `swap_tables()`:
1. All Worker queues must be empty (`updated_pk_set == 0`, `removed_pk_set == 0`)
2. Final validation runs with short timeout
3. If `unmatched_rows > 0` at this point → Swap fails

```python
# controller.py - swap_tables()
if len(updated_pk_set) > 0 or len(removed_pk_set) > 0:
    return  # Not ready for swap

# Final validation before actual swap
# Only THIS validation result determines success/failure
```

### Summary

| Validation Type | is_valid = False | Impact |
|-----------------|------------------|--------|
| bulk_import_validation | Critical | Migration fails, unrecoverable |
| apply_dml_events_validation | OK | Temporary, will be retried |
| full_dml_event_validation | OK | Temporary, will be retried |
| Final validation (swap) | Critical | Swap fails if unmatched_rows > 0 |

---

## Related Tables

```sql
-- Bulk import validation status (with checkpoint support)
CREATE TABLE bulk_import_validation_status (
    id int PRIMARY KEY AUTO_INCREMENT,
    migration_id int,
    chunk_end_pk bigint,
    is_valid bool,
    created_at datetime
);

-- Apply DML events validation status
CREATE TABLE apply_dml_events_validation_status (
    id int PRIMARY KEY AUTO_INCREMENT,
    migration_id int,
    last_validated_timestamp bigint,
    is_valid bool,
    created_at datetime
);

-- Full DML event validation status (with checkpoint support)
CREATE TABLE full_dml_event_validation_status (
    id int PRIMARY KEY AUTO_INCREMENT,
    migration_id int,
    target_end_timestamp bigint,
    last_validated_timestamp bigint,
    is_valid bool,
    created_at datetime
);

-- Unmatched rows (validation failures)
CREATE TABLE unmatched_rows (
    id int PRIMARY KEY AUTO_INCREMENT,
    source_pk bigint,
    migration_id int,
    unmatch_type varchar(128)  -- 'NOT_UPDATED' or 'NOT_REMOVED'
);
```
