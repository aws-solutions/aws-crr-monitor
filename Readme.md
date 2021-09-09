# S3 Cross Region Replication Monitor

A solution describing S3 Cross Region Replication Monitoring.

## Overview
Amazon Simple Storage Service (Amazon S3) offers cross-region replication, a bucket-level feature that enables automatic, asynchronous copying of objects across buckets in different AWS Regions. This feature can help companies minimize latency when accessing objects in different geographic regions, meet compliance requirements, and for operational purposes. Amazon S3 encrypts all data in transit across AWS Regions using SSL, and objects in the destination bucket are exact replicas of objects in the source bucket. For more information on cross-region replication, see the Amazon S3 Developer Guide. AWS customers can retrieve the replication status of their objects manually or use an Amazon S3 inventory to generate metrics on a daily or weekly basis.

To help customers more proactively monitor the replication status of their Amazon S3 objects, AWS offers the Cross-Region Replication Monitor (CRR Monitor) solution. The CRR Monitor automatically checks the replication status of Amazon S3 objects across all AWS Regions in a customers’ account, providing near real-time metrics and failure notifications to help customers proactively identify failures and troubleshoot problems. The solution automatically provisions the necessary AWS services to monitor and view replication status, including AWS Lambda, Amazon CloudWatch, Amazon Simple Notification Service (Amazon SNS), AWS CloudTrail and Amazon DynamoDB, and offers an option to use Amazon Kinesis Firehose to archive replication metadata in Amazon S3.

## CloudFormation Templates
* crr-monitor.template
* crr-agent.template

## Lambda Scripts
* CRRdeployagent.py
* CRRMonitor.py
* CRRHourlyMaint.py
* CRRMonitorHousekeeping.py
* CRRMonitorTrailAlarm.py
* solution-helper.py


## Collection of operational metrics
This solution collects anonymous operational metrics to help AWS improve the quality and features of the solution. For more information, including how to disable this capability, please see the [implementation guide](https://docs.aws.amazon.com/solutions/latest/crr-monitor/appendix.html).


***

Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.




