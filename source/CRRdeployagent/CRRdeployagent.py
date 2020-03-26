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
# Unable to import module? You need to zip CRRdeployagent.py with
# cfn_resource.py!!
import cfn_resource
import json

handler = cfn_resource.Resource()

source_buckets = []

try:
    sts = boto3.client('sts')
    ec2 = boto3.client('ec2')
except Exception as e:
    print(e)
    print('Error creating sts and ec2 clients')
    raise e

local_account = sts.get_caller_identity()['Account']

# Create a hash of regions
REGIONSEL = {}

response = ec2.describe_regions()

for region in response['Regions']:

    REGIONSEL[region['RegionName']] = 0

###########Get Buckets and Agent Region #################

def get_replica_buckets(client):
    print('List Replica Buckets:')
    try:
        list_buckets = client.list_buckets()['Buckets']
        replica_buckets = []
        for i in list_buckets:
            bucket_response = get_bucket_replication(i['Name'], client)
            if 'ReplicationConfigurationError-' != bucket_response \
                    and bucket_response['ReplicationConfiguration']['Rules'][0]['Status'] != 'Disabled':
                source_buckets.append(i['Name'])
                dest_bucket_arn = bucket_response['ReplicationConfiguration']['Rules'][0]['Destination']['Bucket']
                replica_buckets.append(dest_bucket_arn.split(':', 5)[5])
    except Exception as e:
        print(e)
        raise e
    return replica_buckets

def get_bucket_replication(bucket_name, client):
    try:
        response = client.get_bucket_replication(
            Bucket=bucket_name
        )
    except Exception as e:
        print(e)
        response = "ReplicationConfigurationError-"
    return response

#Gets the list of agent regions for Agent deployment

def get_agent_regions():
    try:
        client = boto3.client('s3')
        replica_buckets = get_replica_buckets(client)
        agent_set = set([])
        for bucket in replica_buckets:
            try:
                response = client.head_bucket(
                    Bucket=bucket
                )
                region = response['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']
                if region == None:
                    region = 'us-east-1'
                agent_set.add(region)
            except Exception as e:
                print('Unable to get region for bucket ' + bucket)
                print(e)

        for bucket in source_buckets:
            try:
                response = client.head_bucket(
                    Bucket=bucket
                )
                region = response['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']
                if region == None:
                    region = 'us-east-1'
                agent_set.add(region)
            except Exception as e:
                print('Unable to get region for bucket ' + bucket)
                print(e)

        agent_regions = list(agent_set)
        print('get_agent_regions: agent_regions = ')
        print(*agent_regions, sep = "\n")
    except Exception as e:
        print(e)
        raise e
    return agent_regions

# =====================================================================
# CREATE
#
@handler.create

def create_agent(event, context):
    # print(json.dumps(event))
    # For manager/agent account you will receive:
    # - Topic
    # - CRRQueueArn
    # - MyAccountId
    # For agent-only (remote) account you will receive:
    # - MyAccountId
    # - CRRMonitorAccount
    #
    print(json.dumps(event))
    monitor_account = ''
    topic_name = ''
    queue_arn = ''
    agent_accounts = []


    if 'Topic' in event["ResourceProperties"]:
        topic_name = event["ResourceProperties"]["Topic"] # SNS topic

    if 'CRRQueueArn' in event["ResourceProperties"]:
        queue_arn = event["ResourceProperties"]["CRRQueueArn"]

    if 'CRRMonitorAccount' in event["ResourceProperties"]:
        monitor_account = event["ResourceProperties"]["CRRMonitorAccount"]

    if 'AgentAccounts' in event["ResourceProperties"]:
        agent_accounts = event["ResourceProperties"]["AgentAccounts"]


    # Default value for returning resourceid
    physical_resource_id = {'PhysicalResourceId': 'CRRMonitorAgent-Deployed'}

    # Configure each region for monitoring based on what we found.
    for region in REGIONSEL:

        print('Deploying in ' + region)

        agent_creator(region, topic_name, queue_arn, monitor_account, agent_accounts)

    return physical_resource_id

def agent_creator(agt_region, topic_name, queue_arn, monitor_account, agent_accounts):

    rule = 'CRRRemoteAgent'

    if not monitor_account:
        rule = 'CRRAgent'

    boto3.setup_default_session(region_name=agt_region)
    # -----------------------------------------------------------------
    # Create client connections
    #
    try:
        cwe = boto3.client('events')
        sns = boto3.client('sns')
    except Exception as e:
        print(e)
        print('Error creating clients for ' + agt_region)
        raise e

    try:
        cwe.put_rule(
            Description='Fires CRRMonitor for S3 events that indicate an object has been stored.',
            Name=rule,
            EventPattern="{ \"detail-type\": [ \"AWS API Call via CloudTrail\" ], \"detail\": { \"eventSource\": [ \"s3.amazonaws.com\"], \"eventName\": [ \"PutObject\", \"CopyObject\", \"CompleteMultipartUpload\" ] } }",
            State='DISABLED'
        )
    except Exception as e:
        print(e)
        print('Error creating CW Event rule')
        raise e

    if not monitor_account:
        print('Creating agent for a monitor/agent account in region ' + agt_region)
        topic = topic_name + "-" + agt_region

        # -----------------------------------------------------------------
        # Note: duplication is not a concern - we will replace the rule and
        # topic if they already exist
        #
        # Create the CloudWatch Event rule in a disabled state.
        # Create an SNS topic
        # Add a target to the rule to send to the new SNS topic
        # Enable the rule
        try:

            topicarn = sns.create_topic(Name=topic)['TopicArn']
            sns.set_topic_attributes(
                TopicArn=topicarn,
                AttributeName='Policy',
                AttributeValue='{\
            "Version": "2012-10-17",\
            "Id": "CWEventPublishtoTopic",\
            "Statement": [\
                {\
                  "Sid": "CWEventPublishPolicy",\
                  "Action": [\
                    "SNS:Publish"\
                  ],\
                  "Effect": "Allow",\
                  "Resource": "' + topicarn + '",\
                  "Principal": {\
                    "Service": [\
                      "events.amazonaws.com"\
                    ]\
                  }\
                }\
              ]\
            }\
                    ',

            )
            cwe.put_targets(
                Rule=rule,
                Targets=[
                    {
                        'Id': 'CRRAgent-' + agt_region,
                        'Arn': topicarn
                    }
                ]
            )
            cwe.enable_rule(Name=rule)
        except Exception as e:
            print(e)
            print('Error creating SNS topic and CW Event rule: ' + topic)
            raise e

        # -----------------------------------------------------------------
        # Create cross-region Queue subscription from the SNS end
        # Only when deployed from the Manager account
        #
        try:
            response = sns.subscribe(
                TopicArn=topicarn,
                Protocol='sqs',
                Endpoint=queue_arn
            )

        except Exception as e:
            print(e)
            print('Error subscribing SNS topic ' + topic + ' to SQS Queue ' + queue_arn)
            raise e

        # Grant permissions to the default event bus
        for account in agent_accounts:
            try:
                cwe.put_permission(
                    Action='events:PutEvents',
                    Principal=account,
                    StatementId=account
                )
            except Exception as e:
                print(e)
                print('Error creating Event Bus permissions for ' + account)
                raise e

        return_data = {
            'Data': { 'TopicArn': topicarn },
            'PhysicalResourceId': 'CRRMonitorAgent-' + agt_region
            }

    else:
        print('Creating agent for an agent-only account in region ' + agt_region)
        try:
            cwe.put_targets(
                Rule=rule,
                Targets=[
                    {
                        'Id': 'CRRRemoteAgent-' + agt_region,
                        'Arn': 'arn:aws:events:' + agt_region + ':' + monitor_account + ':event-bus/default'
                    }
                ]
            )
            cwe.enable_rule(Name=rule)
        except Exception as e:
            print(e)
            print('Error creating CW Event target')
            raise e

        return_data = {'PhysicalResourceId': 'CRRMonitorAgent-' + agt_region}

    return return_data

# =====================================================================
# UPDATE
#
@handler.update
def update_agent(event, context):
    event["ResourceProperties"]["Topic"] # SNS topic

    # No update action necessary
    return {}

# =====================================================================
# DELETE
#
@handler.delete
def delete_agent(event, context):
    # For manager/agent account you will receive:
    # - Topic
    # - CRRQueueArn
    # For agent-only (remote) account you will receive:
    # - MyAccountId
    #
    monitor_account = ''
    topic_name = ''
    queue_arn = ''
    agent_accounts = []

    if 'Topic' in event["ResourceProperties"]:
        topic_name = event["ResourceProperties"]["Topic"] # SNS topic

    if 'CRRQueueArn' in event["ResourceProperties"]:
        queue_arn = event["ResourceProperties"]["CRRQueueArn"]
        arnparts = queue_arn.split(':')
        monitor_account = arnparts[4]

    if 'CRRMonitorAccount' in event["ResourceProperties"]:
        monitor_account = event["ResourceProperties"]["CRRMonitorAccount"]

    if 'AgentAccounts' in event["ResourceProperties"]:
        agent_accounts = event["ResourceProperties"]["AgentAccounts"]

    # Get a list of regions where we have source or replica buckets
    agent_regions = get_agent_regions()

    for region in agent_regions:
        agent_deleter(region, topic_name, queue_arn, monitor_account, agent_accounts)

    return {}


def agent_deleter(agt_region, topic_name, queue_arn, monitor_account, agent_accounts):
    #
    # Deletion has to occur in a specific order
    #
    boto3.setup_default_session(region_name=agt_region)
    # -----------------------------------------------------------------
    # Create client connections
    #
    try:
        cwe = boto3.client('events')
        if not monitor_account:
            sns = boto3.client('sns')
    except Exception as e:
        print(e)
        print('Error creating Events client for ' + agt_region)
        raise e

    #------------------------------------------------------------------
    # Remove the CWE rule
    #
    # Rule name is different for Monitor/Agent vs Agent-only
    #
    rule = 'CRRRemoteAgent'

    if not monitor_account:
        rule = 'CRRAgent'
    # 
    # Remove the Targets
    #
    try:
        cwe.remove_targets(
            Rule=rule,
            Ids=[
                rule + '-' + agt_region,
            ]
        )
    except Exception as e:
        print(e)
        print('Failed to remove target ' + rule + ' id ' + rule + '-' + agt_region)

    # For Manager/Agent account, remove the SNS topic
    if not monitor_account:
        topic = topic_name + "-" + agt_region
        print("Delete " + topic + " in " + agt_region)
        # -----------------------------------------------------------------
        # RequestType Delete
        #
        sts = boto3.client('sts')
        myaccount = sts.get_caller_identity()['Account']
        topicarn = 'arn:aws:sns:' + agt_region + ':' + myaccount + ':' + topic
        # Delete the SNS topic
        sns.delete_topic(
            TopicArn=topicarn
        )

    # Delete the CW rule
    cwe.delete_rule(
        Name=rule
    )

    if not monitor_account:
        # Remove permissions to the default event bus
        for account in agent_accounts:
            try:
                cwe.remove_permission(
                    StatementId=account
                )
            except Exception as e:
                print(e)
                print('Error removing Event Bus permissions for ' + account)

    return {}
