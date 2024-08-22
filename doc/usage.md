# Usage

## 1. Create AWS Resources

### IAM Role

Two IAM role is required. One for `ExternalSecrets` to access SecretsManager secret and another for the `monitor` to access CloudWatch metrics. Each role will be attached to separate service accounts.   


Create an IAM role with the following policy:

**sb-osc-external-role**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
              "secretsmanager:GetSecretValue",
              "secretsmanager:DescribeSecret",
              "secretsmanager:ListSecretVersionIds"
            ],
            "Resource": "arn:aws:secretsmanager:REGION:ACCOUNT_ID:secret:SECRET_NAME"
        }
    ]
}
```

**sb-osc-role**
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

### SecretsManager Secret
SB-OSC uses ExternalSecrets with SecretsManager for credentials. Following keys should be defined. 

- `username`: Database username
- `password`: Database password
- `port`: Database port
- `redis_host`: Redis endpoint (k8s Service name)
- `redis_password`: Redis password
- `slack_channel`: Slack channel ID (Optional)
– `slack_token`: Slack app token (Optional)

You can find these keys in [secret.py](../src/config/secret.py)

## 2. Create Destination Table
SB-OSC does not create destination table on its own. Table should be manually created before starting migration.

## 3. Enable Binlog
SB-OSC requires binlog to be enabled on the source database. Please set `binlog_format` to `ROW`

### Other Parameters
- Setting `binlog-ignore-db` to `sbosc` is recommended to prevent SB-OSC from processing its own binlog events.
- Set `range_optimizer_max_mem_size` to `0` or a large value to prevent bad query plans on queries with large `IN` clauses (especially on Aurora v3)

## 4. Run SB-OSC
When all of the above steps are completed, you can start the migration process by installing the [helm chart](../deploy/charts)

```bash
helm install charts sb-osc -n sb-osc --create-namespace

# or
helm -i upgrade charts sb-osc -n sb-osc
```
