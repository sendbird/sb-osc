# Config

## Flow Control
SB-OSC allows user to control the flow of the migration process by setting various parameters. You can set these parameters to apply specific stages based on your environment and requirements.

### skip_bulk_import
If you set this parameter to `True`, SB-OSC will skip the bulk import stage and start from the apply DML events stage. This is useful when you have already copied the data to the destination table and only need to apply DML events. For example, when you create a clone cluster to make an initial copy and replicate changes using SB-OSC, this parameter can be set to `True`. `init_binlog_file` and `init_binlog_position` should be also set when `skip_bulk_import` is `True`, otherwise it will raise an error.

### disable_apply_dml_events
If you set this parameter to `True`, SB-OSC will pause before `apply_dml_events` stage. This is useful when you have additional steps to perform manually before applying DML events.


## Chunk
### max_chunk_count & min_chunk_size
SB-OSC calculates the number of chunks to create based on following formula
```python
chunk_count = min(max_id // min_chunk_size, max_chunk_count)
```

## Worker
### batch_size, thread_count, commit_interval
These parameters control insert throughput of SB-OSC. `batch_size` and `thread_count` are managed similarly. They have min, max values and step size.  

`batch_size` will be set to `min_batch_size` initially and will be increased by `batch_size_step` until it reaches `max_batch_size`. 

`thread_count` will be set to `min_thread_count` initially and will be increased by `thread_count_step` until it reaches `max_thread_count`. 

`commit_interval` is time to sleep after each commit.

### use_batch_size_multiplier
`batch_size_multiplier` is used to increase insert rate on sparse or badly distributed table. If this parameter is set to `True`, SB-OSC will multiply `batch_size` by `batch_size_multiplier` after each successful insert.  

`batch_size_multiplier` is calculated by dividing `batch_size` by actual affected rows of the last insert

`LIMIT batch_size` is applied to the next query to prevent from inserting too many rows at once.

