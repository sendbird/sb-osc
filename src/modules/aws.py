from datetime import datetime
from typing import Union, Dict, List

import boto3

from config import env


class CloudWatch:
    def __init__(self):
        self.client = boto3.client('cloudwatch', region_name=env.AWS_REGION)

    def get_rds_instance_metrics(
            self,
            metric_name,
            instance_id: str,
            start_time: Union[datetime, str],
            end_time: Union[datetime, str],
            statistics,
            unit,
    ) -> Dict:
        """
        :param metric_name: name of the metric
        :param instance_id: instance id
        :param start_time: start datetime you want to get (ex: '2021-09-10 03:57:45.117255')
        :param end_time: end datetime you want to get (ex: '2021-09-10 04:27:45.117255')
        :param statistics: 'SampleCount'|'Average'|'Sum'|'Minimum'|'Maximum'
        :param unit: unit of data
        """
        return self.client.get_metric_statistics(
            Namespace='AWS/RDS',
            MetricName=metric_name,
            Dimensions=[
                {
                    'Name': 'DBInstanceIdentifier',
                    'Value': instance_id
                },
            ],
            StartTime=str(start_time),
            EndTime=str(end_time),
            Period=60,  # seconds
            Statistics=[statistics],
            Unit=unit
        )

    def get_instance_cpu_usages(
            self,
            instance_id: str,
            start_time: Union[datetime, str],
            end_time: Union[datetime, str],
            statistics
    ) -> List[Dict]:
        return self.get_rds_instance_metrics(
            'CPUUtilization', instance_id, start_time, end_time, statistics, 'Percent')['Datapoints']
