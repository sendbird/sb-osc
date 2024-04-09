## Add Index
Secondary indexes on large tables degrades insert performance significantly. SB-OSC handles this problem by allowing users to create indexes after the bulk import stage. Although `ALTER TABLE ... ADD INDEX` command after initial data copy still takes a long time, it is much faster than copying table with those indexes.  

There are a few things to consider when creating indexes after the bulk import stage.

### FreeLocalStorage
Before `ALTER TABLE ... ADD INDEX` command finishes, index is temporarily created in the local storage of the Aurora MySQL instance. The amount of FreeLocalStorage should be greater than the total size of the index being created together. If the FreeLocalStorage is not enough, the index creation will fail with when FreeLocalStorage reaches 0.

### Free Memory (Enhanced Monitoring)
Upon creating an index, the Free Memory as reported by Enhanced Monitoring will decrease. This decrease continues rapidly until it reaches a certain value. However, Aurora has the capability to immediately reclaim memory from FreeableMemory (as observed in CloudWatch), so this should not pose a significant issue. Nonetheless, it is important to monitor and ensure that neither Free Memory nor Freeable Memory reaches zero.

