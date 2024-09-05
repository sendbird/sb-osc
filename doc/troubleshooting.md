# Troubleshooting

This sections provides list of possible issues and solutions that may occur when using SB-OSC. 

### Redis errors
**AuthenticationError**
```
redis.exceptions.AuthenticationError: AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?
```
This error occurs when `redis_password` in `secret.json` is set and `requirepass` in `redis.conf` is not set or empty. Make sure to set the same password in both files.

```
redis.exceptions.AuthenticationError: invalid username-password pair or user is disabled.
```
This error occurs when `redis_password` in `secret.json` is set and `requirepass` in `redis.conf` is set but the password is incorrect. 

**ConnectionError**
You can encounter `redis.exceptions.ConnectionError` when redis host is not set properly. Make sure to set the correct host in `secret.json`.

### no attribute 'logger'
```
AttributeError: 'WorkerManager' object has no attribute 'logger'
AttributeError: 'MetricMonitor' object has no attribute 'logger'
AttributeError: 'EventHandler' object has no attribute 'logger'
```
This error can occur on two occasions. 

SB-OSC starts its process from controller, and controller creates required tables, and sets required redis keys. Other processes waits and restarts until it finishes. Above error can occur during controller's initializing process. This will be resolved shortly after controller finishes initializing process.

However, if there were any problems during controller's initializing process, especially when setting redis keys, it can't be resolved automatically. In this case, you need to delete row in `migration_plan` table so that you can start process with new `migration_id`. Following error log from controller can indicate this problem.
```
     self.source_column_list: list = metadata.source_columns.split(',')
                                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
 AttributeError: 'NoneType' object has no attribute 'split'
```

### TypeError on self.log_file = status['File']
EventHandler uses `SHOW MASTER STATUS` to get the current binlog file. If `SHOW MASTER STATUS` returns 0 rows, this error will occur. Please double-check if binlog is enabled on the source database. 

### ZeroDivisionError: integer division or modulo by zero
```
     chunk_size = max_id // chunk_count
                  ~~~~~~~^^~~~~~~~~~~~~
 ZeroDivisionError: integer division or modulo by zero
```
`chunk_count` is calculated from `min(max_id // min_chunk_size, max_chunk_count)`.
If `min_chunk_size` is larger than `max_id`, `chunk_count` will be 0. In this case, you need to decrease `min_chunk_size`. Restart components after changing the value to apply the change.

### apply_dml_events_validation_batch_size
~~When setting `apply_dml_events_validation_batch_size` there are two factors to consider. Since the binlog resolution is in seconds, if the number of DML events in a second is greater than the batch size, the validation process can hang indefinitely. In this case, it is recommended to increase the batch size.~~  
-> This issue was fixed by [#10](https://github.com/sendbird/sb-osc/pull/10)


Another factor is `max_allowed_packet` of MySQL. Apply DML events stage uses query with IN clause containing `apply_dml_events_validation_batch_size` number of PKs. If the size of this query exceeds `max_allowed_packet`, the query will not return properly. In this case, it is recommended to decrease the batch size. Also, you might need to kill running queries since it may hang indefinitely in this case.

### Monitoring
SB-OSC uses CPU utilization and WriteLatency to determine the optimal `batch_size` and `thread_count`. But in some cases, other metrics such as `RowLockTime`, `DiskQueueDepth` can be more related to cluster's overall performance. We are currently discovering the best combination of metrics to monitor for optimal performance.  
