# EventLoader

EventLoader is a component that loads DML events (INSERT/UPDATE/DELETE) captured from binlog and stored in DB tables, then delivers them to Redis for Worker processing.

## Basic Operation

```
EventHandler (binlog parsing)
    │
    ▼
inserted_pk / updated_pk / deleted_pk tables (DB)
    │
    ▼
EventLoader (loads from DB)
    │
    ▼
Redis updated_pk_set / removed_pk_set
    │
    ▼
Worker (actual data synchronization)
```

### Core Functions

1. **`get_start_timestamp()`**: Retrieves the last loaded timestamp from `apply_dml_events_status` table
2. **`get_max_timestamp()`**: Retrieves the maximum timestamp across all event tables
3. **`get_end_timestamp()`**: Dynamically adjusts batch size (reduces `batch_duration //= 2` if too many events)
4. **`get_pk_batch()`**: Loads PKs within a timestamp range
5. **`load_events_from_db()`**: Main loading logic

---

## Challenging Part 1: Stage Transition with EventHandler

### Stage Flow

```
APPLY_DML_EVENTS → APPLY_DML_EVENTS_PRE_VALIDATION → APPLY_DML_EVENTS_VALIDATION
```

### APPLY_DML_EVENTS → PRE_VALIDATION Transition

`eventhandler.py:231-235`:
```python
def apply_dml_events(self):
    self.start_event_loader()
    if len(self.redis_data.updated_pk_set) == 0 and len(self.redis_data.removed_pk_set) == 0 and \
            self.event_store.last_event_timestamp - self.event_loader.last_loaded_timestamp < 60:
        self.redis_data.set_current_stage(Stage.APPLY_DML_EVENTS_PRE_VALIDATION)
```

Transition conditions:
1. `updated_pk_set == 0`: Worker has processed all updated PKs
2. `removed_pk_set == 0`: Worker has processed all deleted PKs
3. `last_event_timestamp - last_loaded_timestamp < 60`: EventLoader has nearly caught up with binlog (within 60 seconds)

### PRE_VALIDATION → VALIDATION Transition

`eventhandler.py:237-258`:
```python
def apply_dml_events_pre_validation(self):
    self.start_event_loader()
    self.save()
    # ... count queries ...
    if inserted_count + updated_count + deleted_count > 0:
        while self.event_store.last_event_timestamp != self.event_loader.last_loaded_timestamp:
            if self.stop_flag:
                return
            time.sleep(60)
    self.event_loader.set_stop_flag()
    if self.are_indexes_created():
        self.live_mode = True
        self.redis_data.set_current_stage(Stage.APPLY_DML_EVENTS_VALIDATION)
```

Transition conditions:
1. `last_event_timestamp == last_loaded_timestamp`: EventLoader has fully loaded all events
2. Index creation completed

### Key: last_loaded_timestamp Initialization Issue

`eventloader.py:174-188`:
```python
if start_timestamp == 0 or start_timestamp > max_timestamp:
    self.logger.info("No events to load")
    if self.last_loaded_timestamp == 1:
        # Set last loaded timestamp to initial timestamp
        # By updating it here, eventhandler can move to next stage
        # Also it will prevent eventhandler from moving to next stage too early even before loading events
        with self.db.cursor(role='reader') as cursor:
            cursor.execute(f'''
                SELECT last_event_timestamp FROM {config.SBOSC_DB}.event_handler_status
                WHERE migration_id = %s ORDER BY id LIMIT 1
            ''', (self.migration_id,))
            if cursor.rowcount > 0:
                self.last_loaded_timestamp = cursor.fetchone()[0]
```

**Why is this needed?**

To handle the case when there are no DML events at all:

1. EventHandler parses binlog and updates `event_handler_status.last_event_timestamp`
2. But if there are no actual DML events, `inserted_pk`, `updated_pk`, `deleted_pk` tables remain empty
3. EventLoader has nothing to load, so `last_loaded_timestamp` stays at initial value (1)
4. The condition `last_event_timestamp - last_loaded_timestamp < 60` in `apply_dml_events()` is never satisfied

**Solution:**
- When there are no events and `last_loaded_timestamp` is still initial value (1), fetch EventHandler's `last_event_timestamp`
- This allows the condition `last_event_timestamp - last_loaded_timestamp < 60` to be satisfied, enabling progression to next stage

**Two purposes:**
1. Enables progression to next stage even when there are no events
2. Prevents premature stage transition before event loading has started (initial value 1 fails the condition)

---

## Challenging Part 2: Empty Range Jump Logic

### Problem Scenario

Binlog timestamps are recorded in 1-second units. When there are no DML events in a specific timestamp range and events exist much later:

```
Timeline:
[3000] ─── event exists (start_timestamp)
[3001-9999] ─── no events
[10000] ─── next event
```

**Issue with previous logic:**
1. `get_pk_batch(3000, 6000)` called → only loads event at `start_timestamp=3000`
2. No events greater than `start_timestamp` in range → `max_timestamp_in_batch = start_timestamp = 3000`
3. Saves `last_loaded_timestamp = 3000` to `apply_dml_events_status`
4. Next loop: `get_start_timestamp()` → returns `3000`
5. **Infinite loop on same range (Stuck)**

### Solution: Jump to next_timestamp

`eventloader.py:204-208`:
```python
# Save last loaded event timestamp
if max_timestamp_in_batch == start_timestamp and max_timestamp > start_timestamp:
    last_loaded_timestamp = next_timestamp
else:
    last_loaded_timestamp = max_timestamp_in_batch
```

**Condition Analysis:**

| max_timestamp_in_batch | max_timestamp | Meaning | Action |
|------------------------|---------------|---------|--------|
| == start_timestamp | > start_timestamp | No events in current batch, exists later | Jump to next_timestamp |
| == start_timestamp | == start_timestamp | Events only in current batch, none later | Retry same range (wait for new events) |
| > start_timestamp | - | Events exist in current batch | Normal progression |

**Example:**

```
Scenario: start=3000, batch_duration=3000, next event=10000

1. get_pk_batch(3000, 6000) → max_timestamp_in_batch = 3000
2. max_timestamp = 10000 (max across all tables)
3. Check conditions:
   - max_timestamp_in_batch == start_timestamp? → True (3000 == 3000)
   - max_timestamp > start_timestamp? → True (10000 > 3000)
4. last_loaded_timestamp = next_timestamp = 6000 (jump!)
5. Next loop: start_timestamp = 6000
6. get_pk_batch(6000, 9000) → empty range again → jump to 9000
7. get_pk_batch(9000, 12000) → loads event at 10000!
```

### Why Batches Overlap at Timestamp Boundaries

Binlog timestamps have 1-second granularity, so multiple events can share the same timestamp. New events can also arrive at a timestamp that was already processed:

```
Batch 1: [3000, 6000] → max_timestamp_in_batch = 5000 → save last_loaded_timestamp = 5000
Batch 2: [5000, 8000] → starts from 5000, not 5001
```

**Key design: Next batch starts from `last_loaded_timestamp`, not `last_loaded_timestamp + 1`**

This ensures that if new events arrive at timestamp 5000 after Batch 1 completed, Batch 2 will still pick them up.

**When `max_timestamp_in_batch == start_timestamp`:**
- No events with timestamp greater than `start_timestamp` in the current batch
- If `max_timestamp == start_timestamp`: no events beyond this timestamp in entire table yet
  - New events may still arrive at this timestamp from binlog
  - Save `last_loaded_timestamp = start_timestamp` and retry same range
- If `max_timestamp > start_timestamp`: events exist further ahead
  - Safe to jump to `next_timestamp` since binlog has moved past current timestamp
  - No more events will arrive at `start_timestamp`

---

## Related Tables

```sql
-- EventLoader progress status
CREATE TABLE apply_dml_events_status (
    id int PRIMARY KEY AUTO_INCREMENT,
    migration_id int,
    last_loaded_timestamp bigint,  -- last loaded event_timestamp
    created_at datetime
);

-- EventHandler binlog position
CREATE TABLE event_handler_status (
    id int PRIMARY KEY AUTO_INCREMENT,
    migration_id int,
    log_file varchar(128),
    log_pos bigint,
    last_event_timestamp bigint,  -- max timestamp processed from binlog
    created_at datetime
);

-- DML event storage
CREATE TABLE inserted_pk_{migration_id} (source_pk bigint PRIMARY KEY, event_timestamp bigint);
CREATE TABLE updated_pk_{migration_id} (source_pk bigint PRIMARY KEY, event_timestamp bigint);
CREATE TABLE deleted_pk_{migration_id} (source_pk bigint PRIMARY KEY, event_timestamp bigint);
```