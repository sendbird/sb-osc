# Config

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

