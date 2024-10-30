# Usage Guide
This guide will give you instructions on operating SB-OSC during runtime.

### Changing Configurations
When you need to change configurations, you can edit `config.yaml` and `secret.json` files. After editing, you need to restart the containers to apply the changes.
Most of the values in `config.yaml` can be modified after starting the process. 
However, there are some configs that need to be set before starting the process.

Following configurations should be set before starting the process:

**Required fields**
- Values that determine `migration_id` 
  - `source_cluster_id`
  - `source_writer_endpoint` (if `source_cluster_id` is not provided)
  - `destination_writer_endpoint` (if `destination_cluster_id` is not provided)
  - `source_db`
  - `source_table`
  - `destination_db`
  - `destination_table`

**Optional fields**
- `sbosc_db`: SB-OSC database name
- Starting binlog file and position
  - `init_binlog_file`
  - `init_binlog_position`
- Bulk import chunk related config
  - `max_chunk_count`
  - `min_chunk_size`
- Index related config
  - `indexes`
  - `index_created_per_query`

If you want to change these values, you have to modify corresponding data sources (e.g. database, Redis) manually.
For migration_id related values, you need to delete the row in the `migration_plan` table and for index related values, you need to modify `index_creation_status` table.

### Pausing and Resuming
SB-OSC is resumable at any stage of the schema migration process. It saves the current state of each stage to the database and Redis, allowing users to pause and resume the process at any time, as long as binlog retention is sufficient.

To pause the process, you just have to stop all the containers. To resume the process, you need to start the containers again. All checkpoints are saved in the database and Redis, so the process will resume from the last checkpoint.
