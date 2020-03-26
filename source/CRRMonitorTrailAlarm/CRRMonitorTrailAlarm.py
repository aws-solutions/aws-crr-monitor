#!/usr/bin/python
# -*- coding: utf-8 -*-

######################################################################################################################
#  Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           #
#                                                                                                                    #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance    #
#  with the License. A copy of the License is located at                                                             #
#                                                                                                                    #
#      http://www.apache.org/licenses/LICENSE-2.0                                                                    #
#                                                                                                                    #
#  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES #
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    #
#  and limitations under the License.                                                                                #
######################################################################################################################

from __future__ import print_function

import boto3
import json

# Unable to import module? You need to zip CRRdeployagent.py with
# cfn_resource.py!!
import cfn_resource

handler = cfn_resource.Resource()

source_buckets = []

client = {
    's3': { 'service': 's3' },
    'cloudtrail': { 'service': 'cloudtrail'},
    'cloudwatch': { 'service': 'cloudwatch'}
}



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
                clients_to_connect[c]['handle'] = boto3.client(clients_to_connect[c]['service'], region_name=clients_to_connect[c]['region'])
            else:
                clients_to_connect[c]['handle'] = boto3.client(clients_to_connect[c]['service'])
        except Exception as e:
            print(e)
            print('Error connecting to ' + clients_to_connect[c]['service'])
            raise e
    return clients_to_connect

def create_trail(trail_name, trail_log_bucket):
    print('Create Trail: ')
    try:
        response = client['cloudtrail']['handle'].create_trail(
            Name=trail_name,
            S3BucketName=trail_log_bucket,
            IncludeGlobalServiceEvents=True,
            IsMultiRegionTrail=True,
            EnableLogFileValidation=True
        )

        ## Start Logging
        client['cloudtrail']['handle'].start_logging(
            Name=response['TrailARN']
        )
    except Exception as e:
        print(e)
        print('Create Trail')
        raise e


def get_buckets():
    print('List Buckets:')
    try:
        list_buckets = client['s3']['handle'].list_buckets()['Buckets']
        crr_buckets = []
        for i in list_buckets:
            bucket_response = get_bucket_replication(i['Name'])
            if 'ReplicationConfigurationError-' != bucket_response \
                    and bucket_response['ReplicationConfiguration']['Rules'][0]['Status'] != 'Disabled':
                source_buckets.append(i['Name'])
                crr_buckets.append(get_source_bucket_arn(i['Name']))
                crr_buckets.append(get_replica_bucket_arn(bucket_response))
    except Exception as e:
        print(e)
        raise e
    return crr_buckets

def get_source_buckets():
    print('List Buckets:')
    try:
        list_buckets = client['s3']['handle'].list_buckets()['Buckets']
        source_bucket_list = []
        for i in list_buckets:
            bucket_response = get_bucket_replication(i['Name'])
            if 'ReplicationConfigurationError-' != bucket_response \
                    and bucket_response['ReplicationConfiguration']['Rules'][0]['Status'] != 'Disabled':
                source_bucket_list.append(i['Name'])
    except Exception as e:
        print(e)
        raise e
    return source_bucket_list

def get_bucket_replication(bucket_name):
    try:
        response = client['s3']['handle'].get_bucket_replication(
            Bucket=bucket_name
        )
    except Exception as e:
        print(e)
        response = "ReplicationConfigurationError-"
    return response

def get_source_bucket_arn(response):
    try:
        src_bucket = 'arn:aws:s3:::' + response + '/'
    except Exception as e:
        print(e)
        print('SourceBucket')
        raise e
    return src_bucket

def get_replica_bucket_arn(response):
    print(json.dumps(response))
    try:
        dest_bucket_arn = response['ReplicationConfiguration']['Rules'][0]['Destination']['Bucket']
        dest_bucket_prefix = ''
        if 'Prefix' in response['ReplicationConfiguration']['Rules'][0]:
            dest_bucket_prefix = response['ReplicationConfiguration']['Rules'][0]['Prefix']
        replica_bucket = dest_bucket_arn + '/' + dest_bucket_prefix
    except Exception as e:
        print(e)
        print('ReplicaBucket')
        raise e
    return replica_bucket

def put_event_selectors(trail_name,crr_buckets):
    print('Data Events: ')
    try:

        client['cloudtrail']['handle'].put_event_selectors(
            TrailName=trail_name,
            EventSelectors=[
                {
                    'ReadWriteType': 'WriteOnly',
                    'IncludeManagementEvents': True,
                    'DataResources': [
                        {
                            'Type': 'AWS::S3::Object',
                            'Values': crr_buckets
                        },
                    ]
                },
            ]
        )
    except Exception as e:
        print(e)
        print('Data Events Trail')
        raise e

def put_metric_alarm(sns_topic, src_buckets):
    print('Metric Alarms:')
    try:
        for bucket in src_buckets:
            client['cloudwatch']['handle'].put_metric_alarm(
                AlarmName='FailedReplicationAlarm-' + bucket,
                AlarmDescription='Trigger a alarm for Failed Replication Objects.',
                ActionsEnabled=True,
                AlarmActions=[
                    sns_topic,
                ],
                MetricName='FailedReplications',
                Namespace='CRRMonitor',
                Statistic='Sum',
                Dimensions=[
                    {
                        'Name': 'SourceBucket',
                        'Value': bucket
                    },
                ],
                Period=60,
                EvaluationPeriods=1,
                Threshold=0.0,
                ComparisonOperator='GreaterThanThreshold'

            )
    except Exception as e:
        print(e)
        print('Data Events Trail')
        raise e

def put_metric_data(src_buckets):
    print('Metric Data: ')
    try:
        for bucket in src_buckets:
            client['cloudwatch']['handle'].put_metric_data(
                Namespace='CRRMonitor',
                MetricData=[
                    {
                        'MetricName': 'FailedReplications',
                        'Dimensions': [
                            {
                                'Name': 'SourceBucket',
                                'Value': bucket
                            },
                        ],
                        'Value': 0.0
                    },
                ]
            )
    except Exception as e:
        print(e)
        print('Data Events Trail')
        raise e


# =====================================================================
# CREATE
#
@handler.create
def create_trail_alarm(event, context):

    trail_name = event["ResourceProperties"]["trail_name"] # Trail Name
    trail_log_bucket = event["ResourceProperties"]["trail_log_bucket"] # Trail Log
    sns_topic_arn = event["ResourceProperties"]["sns_topic_arn"] # SNS Topic

    ### Trail Creation

    create_trail(trail_name, trail_log_bucket)
    crr_buckets = get_buckets()
    put_event_selectors(trail_name, crr_buckets)

    ### Metric Alarm

    put_metric_data(source_buckets) #Source buckets are derived from get_buckets() call
    put_metric_alarm(sns_topic_arn, source_buckets)
    return { 'PhysicalResourceId': 'CRRMonitorTrailAlarm' }

###### M A I N ######
client = connect_clients(client)


# =====================================================================
# UPDATE
#
@handler.update
def update_trail_alarm(event, context):

    # No update action necessary
    return {}


# =====================================================================
# DELETE
#
@handler.delete
def delete_trail_alarm(event, context):

    trail_name = event["ResourceProperties"]["trail_name"] # Trail Name
    print("Delete TrailName:" + trail_name)
    # -----------------------------------------------------------------
    # Create client connections
    #
    # events
    try:
        ctl = boto3.client('cloudtrail')
        cwe = boto3.client('cloudwatch')

    except Exception as e:
        print(e)
        print('Error creating Events client')
        raise e

    # -----------------------------------------------------------------
    # Remove the Targets
    #
    ctl.delete_trail(
        Name=trail_name
    )

    ###Fetching source bucket details
    source_bucket_list = get_source_buckets()
    for bucket in source_bucket_list:
        cwe.delete_alarms(
            AlarmNames=[
                'FailedReplicationAlarm-' + bucket,
            ]
        )

    return {}
