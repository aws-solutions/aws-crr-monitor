#!/usr/bin/python
# -*- coding: utf-8 -*-

##############################################################################
#  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.   #
#                                                                            #
#  Licensed under the Amazon Software License (the 'License'). You may not   #
#  use this file except in compliance with the License. A copy of the        #
#  License is located at                                                     #
#                                                                            #
#      http://aws.amazon.com/asl/                                            #
#                                                                            #
#  or in the 'license' file accompanying this file. This file is distributed #
#  on an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,        #
#  express or implied. See the License for the specific language governing   #
#  permissions and limitations under the License.                            #
##############################################################################

from __future__ import print_function

import json
import boto3
import os
import logging
from datetime import datetime,timedelta

log = logging.getLogger()
log.setLevel(logging.INFO)

log.debug('Loading function')

def getparm (parmname, defaultval):
    try:
        myval = os.environ[parmname]
        if isinstance(defaultval, int):
            return int(myval)
        else:
            return myval
    except:
        print('Environmental variable \'' + parmname + '\' not found. Using default [' + str(defaultval) + ']')
        return defaultval
#
# Define the DynamoDB table to be used to track replication status.
#   It must be in the same region as this Lambda and should already
#   exist. It is created by the CloudFormation template.
#
ddbtable = getparm('appname','CRRMonitor')
stattable = ddbtable + 'Statistics'
# Stream to kinesis? Must be YES or NO
stream_to_kinesis = getparm('stream_to_kinesis','No')
kinesisfirestream = getparm('kinesisfirestream', ddbtable + 'DeliveryStream')
stack_name = getparm('stack_name','Nil')

timefmt = '%Y-%m-%dT%H:%M:%SZ'
roundTo = getparm('roundto', 300) # 5 minute buckets for CW metrics
purge_thresh = getparm('purge_thresh', 24) # threshold in hours
client={
    'cw': { 'service': 'cloudwatch' },
    'ddb': { 'service': 'dynamodb'}
}
# optionally include firehose
if stream_to_kinesis != 'No':
    #kinesisfirestream = ddbtable + 'DeliveryStream'
    client['firehose'] = { 'service': 'firehose' }

# =====================================================================
# connect_clients
# ---------------
# Connect to all the clients. We will do this once per instantiation of
# the Lambda function (not per execution)
# =====================================================================
def connect_clients(clients_to_connect):
    for c in clients_to_connect:
        try:
            if 'region' in clients_to_connect[c]:
                clients_to_connect[c]['handle']=boto3.client(clients_to_connect[c]['service'], region_name=clients_to_connect[c]['region'])
            else:
                clients_to_connect[c]['handle']=boto3.client(clients_to_connect[c]['service'])
        except Exception as e:
            print(e)
            print('Error connecting to ' + clients_to_connect[c]['service'])
            raise e
    return clients_to_connect

def lambda_handler(event, context):
    # -----------------------------------------------------------------
    # save items in S3 - save items
    #
    def save_item(item):
        print('Save Item' + json.dumps(item))
        try:
            response = client['firehose']['handle'].put_record(
                DeliveryStreamName=kinesisfirestream,
                Record={
                    'Data': json.dumps(item) + '\n'
                }
            )
        except Exception as e:
            print(e)
            print('Error saving ' + item['ETag']['S'] + ' from ' + ddbtable)
            raise e

            # -----------------------------------------------------------------

    # -----------------------------------------------------------------
    # post_stats - post statistics to CloudWatch
    #
    def post_stats(item):
        print('Posting statistics to CloudWatch for ' + item['source_bucket']['S'] + ' time bucket ' + item['timebucket']['S'])

        ts=item['timebucket']['S']

        # -------------------------------------------------------------
        # Special Handling: Failed replicatons are reported in the
        # same data format. The destination bucket will be FAILED.
        # Pull these out separately to a different CW metric.
        if item['dest_bucket']['S'] == 'FAILED':
            try:
                client['cw']['handle'].put_metric_data(
                    Namespace='CRRMonitor',
                    MetricData=[
                        {
                            'MetricName': 'FailedReplications',
                            'Dimensions': [
                                {
                                    'Name': 'SourceBucket',
                                    'Value': item['source_bucket']['S']
                                }
                            ],
                            'Timestamp': ts,
                            'Value': int(item['objects']['N'])
                        },
                    ]
                )
            except Exception as e:
                print(e)
                print('Error creating CloudWatch metric FailedReplications')
                raise e

        else:
            try:
                client['cw']['handle'].put_metric_data(
                    Namespace='CRRMonitor',
                    MetricData=[
                        {
                            'MetricName': 'ReplicationObjects',
                            'Dimensions': [
                                {
                                    'Name': 'SourceBucket',
                                    'Value': item['source_bucket']['S']
                                },
                                {
                                    'Name': 'DestBucket',
                                    'Value': item['dest_bucket']['S']
                                }
                            ],
                            'Timestamp': ts,
                            'Value': int(item['objects']['N'])
                        },
                    ]
                )
            except Exception as e:
                print(e)
                print('Error creating CloudWatch metric')
                raise e

            try:
                client['cw']['handle'].put_metric_data(
                    Namespace='CRRMonitor',
                    MetricData=[
                        {
                            'MetricName': 'ReplicationSpeed',
                            'Dimensions': [
                                {
                                    'Name': 'SourceBucket',
                                    'Value': item['source_bucket']['S']
                                },
                                {
                                    'Name': 'DestBucket',
                                    'Value': item['dest_bucket']['S']
                                }
                            ],
                            'Timestamp': ts,
                            'Value': ((int(item['size']['N'])*8)/1024)/(int(item['elapsed']['N'])+1)
                        },
                    ]
                )
            except Exception as e:
                print(e)
                print('Error creating CloudWatch metric')
                raise e

        print ('Statistics posted to ' + ts)

        try:
            client['ddb']['handle'].delete_item(
                TableName=stattable,
                Key={
                    'OriginReplicaBucket': {
                        'S': item['source_bucket']['S'] + ':' + item['dest_bucket']['S'] + ':' + ts
                    }
                }
            )
            print('Purged statistics date for ' + ts)
        except Exception as e:
            print(e)
            print('Error purging from ' + ts)
            raise e

    #======================== post_stats ==============================

    #==================================================================
    # firehose: retrieve all records completed in the last 5 minutes
    # Stream them to firehose
    def firehose(ts):
        begts=ts - timedelta(minutes=5)
        arch_beg = begts.strftime(timefmt)
        arch_end = ts.strftime(timefmt)
        # Set scan filter attrs
        eav = {
                ":archbeg": { "S": arch_beg },
                ":archend": { "S": arch_end }
            }

        print('Reading from ' + ddbtable)
        try:
            response = client['ddb']['handle'].scan(
                TableName=ddbtable,
                ExpressionAttributeValues=eav,
                FilterExpression="end_datetime >= :archbeg and end_datetime < :archend",
                Limit=1000
                )
        except Exception as e:
            print(e)
            print('Table ' + ddbtable + ' scan failed')
            raise e

        print('Archiving items from ' + ddbtable + ' beg>=' + arch_beg + ' end=' + arch_end)

        for i in response['Items']:
            save_item(i)

        while 'LastEvaluatedKey' in response:
            response = client['ddb']['handle'].scan(
                TableName=ddbtable,
                FilterExpression="end_datetime >= :archbeg and end_datetime < :archend",
                ExpressionAttributeValues=eav,
                ExclusiveStartKey=response['LastEvaluatedKey'],
                Limit=1000
                )

            for i in response['Items']:
                print('Items LastEvaluated ' + i['ETag']['S'])
                save_item(i)
    #====================== firehose ==================================

    # What time is it?
    ts = datetime.utcnow()

    # CRRMonitor logs forward (rounds up). We want to read from the last bucket,
    # not the current on. So round down to the previous 5 min interval
    secs = (ts.replace(tzinfo=None) - ts.min).seconds
    rounding = (secs-roundTo/2) // roundTo * roundTo
    ts = ts + timedelta(0,rounding-secs,-ts.microsecond)

    # save the timestamp we created in a str
    statbucket = datetime.strftime(ts, timefmt) # We'll get stats from this bucket
    print('Logging from ' + statbucket)

    # -----------------------------------------------------------------
    # Process Statistics
    #
    # Get the name of the 5 minute stat bucket that we just stopped
    #  logging to, read the data, and delete the record.
    #
    try:
        client['ddb']['handle'].describe_table(
            TableName = stattable
        )
    except Exception as e:
        print(e)
        print('Table ' + stattable + ' does not exist - need to create it')
        raise e

    eav = {
            ":stats": { "S": statbucket }
        }

    try:
        response = client['ddb']['handle'].scan(
            TableName=stattable,
            ExpressionAttributeValues=eav,
            FilterExpression="timebucket <= :stats",
            ConsistentRead=True
            )
    except Exception as e:
        print(e)
        print('Table ' + ddbtable + ' scan failed')
        raise e

    if len(response['Items']) == 0:
        print('WARNING: No stats bucket found for ' + statbucket)

    for i in response['Items']:
        post_stats(i)

    while 'LastEvaluatedKey' in response:
        try:
            response = client['ddb']['handle'].scan(
                TableName=ddbtable,
                FilterExpression="timebucket <= :stats",
                ExpressionAttributeValues=eav,
                ExclusiveStartKey=response['LastEvaluatedKey'],
                ConsistentRead=True
                )
        except Exception as e:
            print(e)
            print('Table ' + ddbtable + ' scan failed')
            raise e

        for i in response['Items']:
            post_stats(i)

    # Archive to firehose
    if stream_to_kinesis == 'Yes':
        firehose(ts)

######## M A I N ########
client = connect_clients(client)
