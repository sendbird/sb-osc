# SB-OSC

**Sendbird's online schema migration for Aurora MySQL**

SB-OSC is an online schema change tool for Aurora MySQL databases, designed to dramatically improve performance on large
tables by leveraging multithreading in all stages of schema migration process.

It also provides seamless pausing and resuming of tasks to adeptly handle extended operation times of large table schema
changes, along with a built-in monitoring system to dynamically control its heavy DML load based on Aurora's performance
metrics.

SB-OSC is designed to overcome the limitations that existing migration tools face with large-scale tables,
significantly reducing the operational overhead associated with managing large tables.

Please visit our [blog](https://sendbird.com/blog/sb-osc-sendbird-online-schema-change) for more information.

## Takeaways

SB-OSC has its own unique features that differentiate it from existing schema migration tools such as `pt-osc` and `gh-ost`.

### Multithreading

SB-OSC is designed to leverage multithreading in all stages of the schema migration process, bulk import (initial table
copy), binlog event processing, and DML event application.

For binlog event processing, SB-OSC processes binlog files in parallel, which enables it to handle large tables with
heavy write loads.

### Resumable

SB-OSC is resumable at any stage of the schema migration process. It saves the current state of each stage to database
and Redis, allowing users to pause and resume the process at any time, as log as binlog retention is sufficient.

### Operation Class

SB-OSC supports operation classes that can override main queries used in the schema migration process. This feature
allows users to customize queries for specific tables such as data retention, table redesign, and more.

Also, it provides operation class that allows replication cross different Aurora clusters which can be used in various
scenarios such as cross-region replication, cross-account replication, clone cluster replication, etc.

[Guide for operation class](doc/operation-class.md)

### Data Validation

SB-OSC provides strong data validation features to ensure data consistency between the source and destination tables. It
validates both the bulk import and DML event application stages, and attempts to recover from any inconsistencies.

### Index Creation Strategy

SB-OSC allows users to create indexes after the bulk import stage, which can significantly reduce the time required for
the initial table copy. This feature is especially useful for large tables with many indexes.

### Monitoring

SB-OSC has a built-in monitoring system that dynamically controls its heavy DML load based on Aurora's performance
metrics. This feature makes SB-OSC more reliable on production environments, since it will automatically adjust its DML
load when production traffic increases.

## Requirements

SB-OSC is designed to work with Aurora MySQL database, and it's an EKS-based tool.

It requires the following resources to run:

- Aurora MySQL database (v2, v3)
- EKS cluster
- AWS SecretsManager secret
- IAM role

SB-OSC accepts `ROW` for binlog format. It is recommended to set `binlog-ignore-db` to `sbosc` to prevent SB-OSC from
processing its own binlog events.

- `binlog_format` set to `ROW`
- `binlog-ignore-db` set to `sbosc` (Recommended)

Detailed requirements and setup instructions can be found in the [usage guide](deploy/README.md).

## Performance

SB-OSC shows high performance on both binlog event processing and bulk import. Following are specs of tables used for
performance testing:

| Table Alias | Avg Row Length (Bytes) | Write IOPS (IOPS/m) |
|:-----------:|-----------------------:|--------------------:|
|      A      |                     57 |                	149 |
|      B      |                    912 |                	502 |
|      C      |                     61 |              3.38 K |
|      D      |                    647 |              17.9 K |
|      E      |                   1042 |              24.4 K |
|      F      |                     86 |               151 K |
|      G      |                   1211 |              60.7 K |

**Avg Row Length**: `avg_row_length` from `information_schema.TABLES`  
**Write IOPS**: Average increase of `count_write` from `performance_schema.table_io_waits_summary_by_table` per
minute.

All tables were in the same Aurora MySQL v3 cluster

### Binlog Event Processing

Following are read throughput of binlog event processing in read bytes per minute. By comparing read throughput to total
binlog creation rate of the cluster, we can see whether SB-OSC can catch up DML events or not.

**Total Binlog Creation Rate**: 144 (MB/m)

|      Table Alias       |  A  |  B  |  C  |  D  |  E  |  F  |  G  |
|:----------------------:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Read Throughput (MB/m) | 513 | 589 | 591 | 402 | 466 | 361 | 305 |

Result shows that SB-OSC can catch up DML events on tables with very high write load.

### Bulk Import

To provide general insight on bulk import performance, the test was conducted on table `A` with no secondary indexes,
and no additional traffic.

Actual performance of bulk import can vary depending on the number of secondary indexes, the number of rows, column
types,
production traffic, etc.

Following are the results of bulk import performance based on instance sizes:

| Instance Type | Insert Rate (rows/s) | Network Throughput (Bytes/s) | Storage Throughput (Bytes/s) | CPU Utilization (%) |
|:-------------:|---------------------:|-----------------------------:|-----------------------------:|--------------------:|
|  r6g.2xlarge  |               42.3 K |                       27.2 K |                        457 M |                55.0 |
|  r6g.4xlarge  |               94.0 K |                       45.9 K |                        900 M |                51.9 |
|  r6g.8xlarge  |                158 K |                       72.2 K |                       1.39 G |                44.6 |

Insert rate, network throughput, and storage throughput are the average values calculated from CloudWatch metrics.

### Comparison with gh-ost

We've compared total migration time of SB-OSC and gh-ost on following conditions:

- Table `C` with ~200M rows
- Aurora MySQL v3 cluster, r6g.8xlarge instance
- 2 secondary indexes
- `batch_size` (`chunk-size` for gh-ost): 50000
- (gh-ost) `--allow-on-master`

**w/o traffic**

|  Tool  | Total Migration Time | CPU Utilization (%) |
|:------:|---------------------:|--------------------:|
| SB-OSC |                  22m |                60.6 |
| gh-ost |               1h 52m |                19.7 |

**w/ traffic**

Traffic was generated only to table `C` during the migration. (~1.0K inserts/s, ~0.33K updates/s, ~0.33K deletes/s)

|  Tool  | Total Migration Time | CPU Utilization (%) |
|:------:|---------------------:|--------------------:|
| SB-OSC |                  27m |                62.7 |
| gh-ost |                  1d+ |                27.4 |

For gh-ost, we interrupted the migration at 50% (~12h) since ETA kept increasing.

## Limitations

- **Necessity of Integer Primary Keys**
  SB-OSC performs multithreading based on integer primary keys (PKs) during the bulk import phase. This approach,
  designed around batch processing and other operations utilizing integer PKs, means SB-OSC cannot be used with tables
  that do not have integer PKs.


- **Updates on Primary Key**
  SB-OSC replicates records from the original table based on the PK for applying DML events. Therefore, if updates occur
  on the table's PK, it can be challenging to guarantee data integrity.


- **Binlog Resolution**
  SB-OSC is limited by the fact that binlog's resolution is in seconds. While this doesn't significantly impact most
  scenarios due to SB-OSC's design, it can affect the logic based on timestamps when excessive events occur within a
  second.


- **Reduced Efficiency for Small Tables**
  For small tables, the initial table creation, chunk creation, and the multi-stage process of SB-OSC can act as
  overhead, potentially slowing down the overall speed. Therefore, applying SB-OSC to small tables may not be as
  effective.
