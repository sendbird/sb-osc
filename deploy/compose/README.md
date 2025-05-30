# Deploying with Docker Compose

## 1. Create IAM Role

### IAM Role

IAM role is required for the `monitor` to access CloudWatch metrics.  

Create an IAM role with the following policy:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
              "cloudwatch:GetMetricStatistics"
            ],
            "Resource": "*"
        }
    ]
}
```

Attach this role to the instance where SB-OSC is running.

## 2. Write Config Files
You have to write three config files for SB-OSC to run properly. 

### `config.yaml`
This files contains the configuration for SB-OSC. You can find the template in [config.yaml](config.yaml).  
All values are loaded into `Config` class in [config.py](../../src/config/config.py).

### `secret.json` 
This file contains the credentials for the database, redis, and slack. You can find the template in [secret.json](secret.json).  All values are loaded into `Secret` class in [secret.py](../../src/config/secret.py).

- `username`: Database username
- `password`: Database password
- `port`: Database port
- `redis_host`: Redis endpoint (Docker container name)
- `redis_password`: Redis password (Optional)
- `slack_channel`: Slack channel ID (Optional)
– `slack_token`: Slack app token (Optional)

`redis_password` is optional. Keep in mind that if you set a password in `redis.conf`, you should set the same password in `secret.json`.

### `redis.conf`
This file contains the configuration for the Redis server. You can find the template in [redis.conf](redis.conf).  
- `requirepass ""`: Match the `redis_password` set in `secret.json`. 
  - If `requirepass ""` is set, this means that the Redis server does not require a password. Fill in the password between the quotes to set a password.
- `appendonly yes`: Enable AOF persistence
- `save ""`: Disable RDB persistence

## 3. Create Destination Table
SB-OSC does not create destination table on its own. Table should be manually created before starting migration.

## 4. Enable Binlog
SB-OSC requires binlog to be enabled on the source database. Please set `binlog_format` to `ROW`

### Other Parameters
- Setting `binlog-ignore-db` to `sbosc` is recommended to prevent SB-OSC from processing its own binlog events.
- Set `range_optimizer_max_mem_size` to `0` or a large value to prevent bad query plans on queries with large `IN` clauses (especially on Aurora v3)

## 5. Run SB-OSC
When all of the above steps are completed, you can start the migration process by running docker compose.  

Please double-check if the `docker-compose.yml` file is correctly configured (ex. `image`, `AWS_REGION`, etc.)
