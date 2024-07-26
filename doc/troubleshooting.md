# Troubleshooting

This sections provides list of possible issues and solutions that may occur when using SB-OSC. 

### apply_dml_events_validation_batch_size
~~When setting `apply_dml_events_validation_batch_size` there are two factors to consider. Since the binlog resolution is in seconds, if the number of DML events in a second is greater than the batch size, the validation process can hang indefinitely. In this case, it is recommended to increase the batch size.~~  
-> This issue was fixed by [#10](https://github.com/sendbird/sb-osc/pull/10)


Another factor is `max_allowed_packet` of MySQL. Apply DML events stage uses query with IN clause containing `apply_dml_events_validation_batch_size` number of PKs. If the size of this query exceeds `max_allowed_packet`, the query will not return properly. In this case, it is recommended to decrease the batch size. Also, you might need to kill running queries since it may hang indefinitely in this case.

### Monitoring
SB-OSC uses CPU utilization and WriteLatency to determine the optimal `batch_size` and `thread_count`. But in some cases, other metrics such as `RowLockTime`, `DiskQueueDepth` can be more related to cluster's overall performance. We are currently discovering the best combination of metrics to monitor for optimal performance.  
