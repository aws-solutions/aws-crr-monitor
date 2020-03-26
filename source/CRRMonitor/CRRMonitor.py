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

import json
import boto3
from botocore.exceptions import ClientError
import os
from datetime import datetime, timedelta
import urllib.request

def getparm(parmname, defaultval):
    try:
        myval = os.environ[parmname]
        print('Environmental variable \'' + parmname + '\' = ' + str(myval))
        if isinstance(defaultval, int):
            return int(myval)
        else:
            return myval
    except:
        print('Environmental variable \'' + parmname + '\' not found. Using default [' + \
            str(defaultval) + ']')
        return defaultval

# =====================================================================
# Configuration
#
# appname: the names of AWS resources are derived from this. It is not
# recommended that you change this from the default 'CRRMonitor'
appname = getparm('appname', 'CRRMonitor')

# maxtask: Tune this parameter to get the most effective use of a single
# instance of your lambda. It should be roughly 300,000 divided by the average
# time required to process a single SQS record (160ms). Example: if it takes an
# average of 500ms to process a single SQS record you would set this to
# 300 / 0.5 = 600. This parameter tells the lambda when to ask for help:
# If the queue depth is > maxtask it will spawn a copy of itself.
maxtask = getparm('maxtask', 1800)

# maxspawn: This parameter limits how many copies of itself the lambda
# can spawn. This should not allow you to exceed your maximum concurrent
# lambda execution limit (default 100). By default the lambda is set
# to execute every minute and time out after 5. With a default maxspawn
# of 25 this will allow 100 concurrent lambdas to execute. This should
# allow capacity of 200 events per second at an average processing time
# of 500ms per event, or 100 CRR replications per second. Scale and
# request limits accordingly.
maxspawn = getparm('maxspawn', 20)

# How long to keep records for completed transfers
purge_thresh = getparm('purge_thresh', 24)

# DEBUG
DEBUG = getparm('debug', 0)

# VERSION_ID: The version of this solution
VERSION_ID = getparm('SolutionVersion', "").strip()

# ANONYMOUS_SOLUTION_ID: An anonymous identifier for this instance of the solution
ANONYMOUS_SOLUTION_ID = getparm('UUID', "").strip()

# SEND_ANONYMOUS_USAGE_METRIC: A flag indicating whether the solution should
# report anonymous usage metrics to AWS
SEND_ANONYMOUS_USAGE_METRIC = (getparm('AnonymousUsage', 'No') == 'Yes')

# Make sure the VERSION_ID and ANONYMOUS_SOLUTION_ID are valid
if VERSION_ID is None or VERSION_ID == "":
    SEND_ANONYMOUS_USAGE_METRIC = False

if ANONYMOUS_SOLUTION_ID is None or ANONYMOUS_SOLUTION_ID == "":
    SEND_ANONYMOUS_USAGE_METRIC = False

#
# ddbtable and stattable: name of the DynamoDB tables. The tables are
# created in the CloudFormation stack and defaults to the value of appname.
# Do not change this without changing the template.
ddbtable = appname
stattable = ddbtable + 'Statistics'
# queue: name of the SQS queue. Derived from the appname. The SQS queue
# is created in the CloudFormation template. Do not change this without
# changing the template
queue = appname + 'Queue'
# timefmt: used to format timestamps. Do not change.
timefmt = '%Y-%m-%dT%H:%M:%SZ'
# client: defines the api client connections to create
client={
    'ddb': {'service': 'dynamodb'},
    'sqs': {'service': 'sqs'},
    'lbd': {'service': 'lambda'}
}
s3client = {} # will hold client handle for s3 per region
initfail = {} # hash of source buckets to handle FAILED counter initialization

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

def message_handler(event):
    def log_statistics(Src, Dst, Tstamp, Size, ET, roundTo):
        # -------------------------------------------------------------
        # Derive the statistic bucket from source/dest and time bucket
        # (5 minute rolling window)
        #
        statbucket = Src + ':' + Dst
        ts = datetime.strptime(Tstamp, timefmt)
        secs = (ts.replace(tzinfo=None) - ts.min).seconds
        rounding = (secs+roundTo/2) // roundTo * roundTo
        ts = ts + timedelta(0, rounding-secs, -ts.microsecond)
        timebucket = datetime.strftime(ts, timefmt)
        statbucket += ':' + timebucket
        # -------------------------------------------------------------
        # Init a dict to use to hold our attrs for DDB
        stat_exp_attrs = {}
        # -------------------------------------------------------------
        # Build the DDB UpdateExpression
        stat_update_exp = 'SET timebucket = :t, source_bucket = :o, dest_bucket = :r ADD objects :a, size :c, elapsed :d'
        # -------------------------------------------------------------
        # push the first attr: s3Object
        stat_exp_attrs[':a'] = {'N': '1'}
        stat_exp_attrs[':c'] = {'N': Size}
        stat_exp_attrs[':d'] = {'N': ET}
        stat_exp_attrs[':t'] = {'S': timebucket}
        stat_exp_attrs[':o'] = {'S': Src}
        stat_exp_attrs[':r'] = {'S': Dst}
        
        # Update the DDB table
        try:
            response = client['ddb']['handle'].update_item(
                TableName=stattable,
                Key={'OriginReplicaBucket': {'S': statbucket}},
                UpdateExpression=stat_update_exp,
                ExpressionAttributeValues=stat_exp_attrs)
        except Exception as e:
            print(e)
            print('Table ' + stattable + ' update failed')
            raise e

        # Initialize a counter for failed replications for the source bucket
        if not Src in initfail:
            initfail[Src] = 'foo'
        if Dst != 'FAILED' and initfail[Src] != timebucket:
            print('Initializing FAILED bucket for ' + Src + ':' + timebucket)
            statbucket = Src + ':FAILED:' + timebucket
            stat_exp_attrs = {}
            # -------------------------------------------------------------
            # Build the DDB UpdateExpression
            stat_update_exp = 'SET timebucket = :t, source_bucket = :o, dest_bucket = :r ADD objects :a, size :c, elapsed :d'
            # -------------------------------------------------------------
            # push the first attr: s3Object
            stat_exp_attrs[':a'] = {'N': '0'}
            stat_exp_attrs[':c'] = {'N': '1'}
            stat_exp_attrs[':d'] = {'N': '1'}
            stat_exp_attrs[':t'] = {'S': timebucket}
            stat_exp_attrs[':o'] = {'S': Src}
            stat_exp_attrs[':r'] = {'S': 'FAILED'}

            try:
                response = client['ddb']['handle'].update_item(
                    TableName=stattable,
                    Key={'OriginReplicaBucket': {'S': statbucket }},
                    UpdateExpression=stat_update_exp,
                    ExpressionAttributeValues=stat_exp_attrs)
                initfail[Src] = timebucket
            except Exception as e:
                print(e)
                print('Table ' + stattable + ' update failed')
                raise e

        #print('Stats written to ' + statbucket)

    # So this will work with CloudWatch Events directly or via SNS, let's look
    #   at the structure of the incoming JSON. Note that this has not been
    #   tested with CloudWatch events directly, but should be a simple matter.
    #   I kept the code here as it adds no overhead but is a solid flexible
    #   example.
    #
    # A Cloudwatch Event looks like event[event json]
    # An SNS notification looks like event['Records'][0][event json]
    # print("Received raw event: " + json.dumps(event, indent=2))

    # Create a reference in evdata that points to the correct element in the
    #   event dictionary
    if 'detail-type' in event:
        evdata = event
    elif 'Records' in event:
        # An SNS notification will have another layer in the dict. Look for
        #   EventSource = aws:sns. Otherwise generate an exception and get out.
        if event['Records'][0]['EventSource'] == 'aws:sns':
            #print('Message is ' + event['Records'][0]['Sns']['Message'])
            evdata = json.loads(event['Records'][0]['Sns']['Message'])
            #print("Message event: " + json.dumps(evdata, indent=2))

        else:
            # Unrecognized event format: uncomment print statements to
            #    identify the format and enhance this logic. At the end of
            #    the day, evdata must contain the dict for the event record
            #    of the Cloudwatch log event for the S3 update notification
            print('Error: unrecognized event format received')
            raise Exception('Unrecognized event format')

    elif 'MessageId' in event:
        evdata = json.loads(event['Message'])
    else:
        evdata = event

    if DEBUG > 1:
        print(json.dumps(evdata))

    #-----------------------------------------------------------------
    # Quietly ignore all but PutObject
    #
    if evdata['detail']['eventName'] != 'PutObject':
        if DEBUG > 0:
            print('Ignoring ' + evdata['detail']['eventName'] + ' event')
        return

    #-----------------------------------------------------------------
    #
    # Collect the data we want for the DynamoDB table
    #
    region = evdata['region']
    bucket = evdata['detail']['requestParameters']['bucketName']
    key = evdata['detail']['requestParameters']['key']

    # This timestamp is from the CW Event record and is most accurate
    now = evdata['detail']['eventTime']

    # Init a dict to use to hold our attrs for DDB
    ddb_exp_attrs = {}
    # Build th e DDB UpdateExpression
    ddb_update_exp = 'set s3Object = :a'
    # push the first attr: s3Object
    ddb_exp_attrs[':a'] = {'S': key}


    # establish s3 client per region, but only once.
    if not region in s3client:
        s3client[region] = boto3.client('s3', region)

    # -----------------------------------------------------------------
    # Do a head_object. If the object no longer exists just return.
    #
    try:
        response = s3client[region].head_object(
            Bucket=bucket,
            Key=key
            )
    except ClientError as e:
        #  {  "Error": {
        #         "Code": "403",
        #         "Message": "Forbidden"
        #     },
        #     "ResponseMetadata": {
        #         "RequestId": "B7C8873E3C067128",
        #         "HostId": "kYARs5PKMuah57ewyzYq6l5laO4xu9fcWFYVnEPLMHeqNSF4yLhrYIhbbUT0Tw7hp3f2PgCQO9E=",
        #         "HTTPStatusCode": 403,
        #         "HTTPHeaders": {
        #             "x-amz-request-id": "B7C8873E3C067128",
        #             "x-amz-id-2": "kYARs5PKMuah57ewyzYq6l5laO4xu9fcWFYVnEPLMHeqNSF4yLhrYIhbbUT0Tw7hp3f2PgCQO9E=",
        #             "content-type": "application/xml",
        #             "transfer-encoding": "chunked",
        #             "date": "Tue, 25 Sep 2018 11:58:48 GMT",
        #             "server": "AmazonS3"
        #         },
        #         "RetryAttempts": 0
        #     }
        #   }

        if e.response['Error']['Code'] == '403':
            print('IGNORING: CRRMonitor does not have access to Object - ' + \
                evdata['detail']['requestParameters']['bucketName'] + '/' + \
                evdata['detail']['requestParameters']['key'])
        elif e.response['Error']['Code'] == '404':
            print('IGNORING: Object no longer exists - ' + \
                evdata['detail']['requestParameters']['bucketName'] + '/' + \
                evdata['detail']['requestParameters']['key'])

        else:
            # Need to improve this to recognize specifically a 404
            print('Unhandled ClientError ' + str(e))
            print(json.dumps(e.response))

        #print('Removing from queue / ignoring')
        return

    except Exception as e:
        # Need to improve this to recognize specifically a 404
        print('Unandled Exception ' + str(e))
        print('Removing from queue / ignoring')
        return


    # 2) check that the x-amz-replication-status header is present
    #    response['ResponseMetadata']['HTTPHeaders']['x-amz-replication-status']
    #
    # Note that this function is only called when an object is written. Assume that
    #   the object was written and the x-amz-replication-status is a final status for
    #   this object in this bucket. So, if it is the source it can be COMPLETED, PENDING,
    #   or FAILED. If it is the replica it can only be REPLICA.
    #
    # That in mind, the update date/time for the REPLICA will always be definitive for
    #   the end_datetime column
    #
    # Conversely, the source object is always definitive for the start_datetime.
    #
    # Code must not assume that the events (source and dest) are processed in the correct
    #   order. Any process consuming the DynamoDB table should do their own Elapsed Time
    #   calculation.
    #
    # Reference the dict we want for clarity in the code
    headers = response['ResponseMetadata']['HTTPHeaders']

    # If this object has no x-amz-replication-status header then we can leave
    if 'x-amz-replication-status' not in headers:
        # This is not a replicated object - get out
        if DEBUG > 0:
            print('Not a replicated object')
        return()

    # repstatus is a pointer to the headers (for code clarity)
    repstatus = headers['x-amz-replication-status']

    # -----------------------------------------------------------------
    # Verify that the DynamoDB table exists. Note: we could create it
    #  but that takes so long that the lambda function may time out.
    #  Better to create it in the CFn template and handle this as a
    #  failure condition
    #
    try:
        response = client['ddb']['handle'].describe_table(
            TableName=ddbtable
        )
    except Exception as e:
        print(e)
        print('Table ' + ddbtable + ' does not exist - need to create it')
        raise e

    # Update object size
    objsize = headers['content-length']
    ddb_update_exp += ', ObjectSize = :s'
    ddb_exp_attrs[':s'] = {'N': objsize}

    ETag = {'S': headers['etag'][1:-1] + ':' + headers['x-amz-version-id'][1:-1]}

    # -----------------------------------------------------------------
    # If the object already has a DDB record get it
    #
    ddbdata = client['ddb']['handle'].get_item(
        TableName=ddbtable,
        Key={'ETag': ETag},
        ConsistentRead=True
        )

    ddbitem = {} # reset the dict
    if 'Item' in ddbdata:
        ddbitem = ddbdata['Item']
        if DEBUG > 4:
            print("DDB record: " + json.dumps(ddbitem, indent=2))

    #
    # Is this a REPLICA? Use timestamp as completion time
    #
    # Note: replica only updates s3Replica, replication_status, and end_datetime.
    #
    # We do this so we don't have to handle conditional update of fields that might get
    # stepped on of the events are processed out of order.
    #
    if repstatus == 'REPLICA':
        # print('Processing a REPLICA object: ' + ETag['S'])
        ddb_update_exp += ', s3Replica = :d'
        ddb_exp_attrs[':d'] = {'S': bucket}
        #print('s3Replica: ' + bucket)

        ddb_update_exp += ', end_datetime = :e'
        ddb_exp_attrs[':e'] = {'S': now} # 'now' is from the event data
        #print('end_datetime: ' + now)

        # Set the ttl
        purge = datetime.strptime(now, timefmt) - timedelta(hours=purge_thresh) # datetime object
        ttl = purge.strftime('%s')
        ddb_update_exp += ', itemttl = :p'
        ddb_exp_attrs[':p'] = {'N': ttl}

        # If this is a replica then status is COMPLETE
        ddb_update_exp += ', replication_status = :b'
        ddb_exp_attrs[':b'] = {'S': 'COMPLETED'}
        #print('replication_status: COMPLETED (implied)')

        if 'start_datetime' in ddbitem and 'crr_rate' not in ddbitem:
            etime = datetime.strptime(now, timefmt) - datetime.strptime(ddbitem['start_datetime']['S'], timefmt)
            etimesecs = (etime.days * 24 * 60 * 60) + etime.seconds
            #print("Calculate elapsed time in seconds")
            crr_rate = int(objsize) * 8 / (etimesecs + 1) # Add 1 to prevent /0 errors
            ddb_update_exp += ', crr_rate = :r'
            ddb_exp_attrs[':r'] = {'N': str(crr_rate)}
            #print('crr_rate: ', crr_rate)

            ddb_update_exp += ', elapsed = :t'
            ddb_exp_attrs[':t'] = {'N': str(etimesecs)}
            #print('elapsed: ', etimesecs)
            log_statistics(
                ddbitem['s3Origin']['S'],
                bucket,
                ddbitem['start_datetime']['S'],
                objsize,
                str(etimesecs),
                300)
    # -----------------------------------------------------------------
    # Or is this a SOURCE? Use timestamp as replication start time
    #
    else:

        ddb_update_exp += ', s3Origin = :f'
        ddb_exp_attrs[':f'] = {'S': bucket}

        # If this is not a replica then do not report status. It's not important and
        # makes the DynamoDB update much more complicated. Just get the start time
        #
        # We also do not care what the status is. If it has a FAILED status we could
        # write code to send a notification, but that's outside our scope.
        if repstatus == 'COMPLETED' or repstatus == 'FAILED' or repstatus == 'PENDING':
            # print('Processing a ORIGINAL object: ' + ETag['S'] + ' status: ' + repstatus)
            ddb_update_exp += ', start_datetime = :g'
            ddb_exp_attrs[':g'] = {'S': now}
            # ---------------------------------------------------------
            # If we already got the replica event...
            #
            if 'end_datetime' in ddbitem and 'crr_rate' not in ddbitem:
                etime = datetime.strptime(ddbitem['end_datetime']['S'], timefmt) - datetime.strptime(now, timefmt)
                etimesecs = (etime.days * 24 * 60 * 60) + etime.seconds
                #print("Calculate elapsed time in seconds")
                crr_rate = int(objsize) * 8 / (etimesecs + 1) # Add 1 to prevent /0 errors
                ddb_update_exp += ', crr_rate = :r'
                ddb_exp_attrs[':r'] = {'N': str(crr_rate)}

                # Set the ttl
                purge = datetime.strptime(ddbitem['end_datetime']['S'], timefmt) - timedelta(hours=purge_thresh) # datetime object
                ttl = purge.strftime('%s')
                ddb_update_exp += ', itemttl = :p'
                ddb_exp_attrs[':p'] = {'N': ttl}

                ddb_update_exp += ', elapsed = :t'
                ddb_exp_attrs[':t'] = {'N': str(etimesecs)}

                log_statistics(
                    bucket,ddbitem['s3Replica']['S'], 
                    ddbitem['end_datetime']['S'], 
                    objsize,
                    str(etimesecs),300)
            # ---------------------------------------------------------
            # We did not yet get the replica event
            #
            else:
                if repstatus == 'FAILED':
                    # If replication failed this is the only time we will see this object.
                    # Update the status to FAILED
                    ddb_update_exp += ', replication_status = :b'
                    ddb_exp_attrs[':b'] = {'S': 'FAILED'}
                    log_statistics(
                        bucket,
                        'FAILED',
                        now,
                        '0',
                        '1',
                        300)

        else:
            print('Unknown Replication Status: ' + repstatus)
            raise Exception('Unknown Replication Status')



# Create a record in the DDB table
    try:
        response = client['ddb']['handle'].update_item(
            TableName=ddbtable,
            Key={'ETag': ETag},
            UpdateExpression=ddb_update_exp,
            ExpressionAttributeValues=ddb_exp_attrs)
    except Exception as e:
        print(e)
        print('Table ' + ddbtable + ' update failed')
        raise e

# =====================================================================
# queue_handler
# -------------
# Main entry point
# Count the SQS queue and manage scale.
# Here's what my event looks like:
# {
#   "account": "SAMPLE12345",
#   "region": "us-east-2",
#   "detail": {},
#   "detail-type": "Scheduled Event",
#   "source": "aws.events",
#   "version": "0",
#   "time": "2017-02-09T13:56:03Z",
#   "id": "a8b4f046-06c5-4b3c-b543-90c3fdaaac14",
#   "resources": [
#       "arn:aws:events:us-east-2:SAMPLE12345:rule/CRRMonitor-2"
#   ]
# }
#
# When I spawn a child process I will change "detail-type" to "Spawned Event"
# and add "child-number", where 0 is the top-level
# =====================================================================
def queue_handler(event, context):
    cnum = 0
    if 'child-number' in event:
        cnum = int(event['child-number'])

    message_floor = cnum * maxtask

    # {
    #   "Attributes": {"ApproximateNumberOfMessages": "1040"},
    #   "ResponseMetadata": {
    #       "RetryAttempts": 0,
    #       "HTTPStatusCode": 200,
    #       "RequestId": "51c43b7e-9b05-59c8-b68e-6a68f3f3b999",
    #       "HTTPHeaders": {
    #           "x-amzn-requestid": "51c43b7e-9b05-59c8-b68e-6a68f3f3b999",
    #           "content-length": "360",
    #           "server": "Server",
    #           "connection": "keep-alive",
    #           "date": "Thu, 09 Feb 2017 12:55:18 GMT",
    #           "content-type": "text/xml"
    #       }
    #   }
    # }
    response = client['sqs']['handle'].get_queue_attributes(
        QueueUrl=queue_endpoint,
        AttributeNames=['ApproximateNumberOfMessages']
        )

    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        print('Bad status from ' + queue + ': ' + response['ResponseMetadata']['HTTPStatusCode'])
        return

    queue_sz = int(response['Attributes']['ApproximateNumberOfMessages'])
    queue_backlog = queue_sz - message_floor

    print('INFO [CNUM-' + str(cnum) + '] Queue is ' + str(queue_sz) + \
        ' deep. Backlog is ' + str(queue_backlog))

    # We subtracted the number of messages for which processes are already
    # running. If the backlog is still too deep them first spawn another child,
    # updating child-number + 1
    if queue_backlog > maxtask:
        # increment child-number (or initialize to 1) in the event dict
        # spawn another lambda, passing the event and context dicts
        if cnum < maxspawn:
            event['child-number'] = cnum + 1
            try:
                client['lbd']['handle'].invoke(
                    FunctionName=context.function_name,
                    InvocationType='Event',
                    Payload=json.dumps(event)
                )
                print('Spawning a child because there are ' + str(queue_sz) + ' messages in the queue. I am child ' + str(cnum) + ' with a max capacity of ' + str(maxtask) + '. Message floor is ' + str(message_floor))

                print('Reproduction successful - child ' + str(cnum+1) + ' spawned')
            except Exception as e:
                print(e)
                print('ERROR[CNUM-' + str(cnum) + '] Failed to reproduce')
                raise e
        else:
            print('WARNING: maxspawn(' + str(maxspawn) + ') exceeded. Not spawning a helper.')

    # -----------------------------------------------------------------
    # Now we get to work. Process messages from the queue until empty
    # or we time out. This is the secret sauce to our horizontal scale
    print('INFO [CNUM-' + str(cnum) + '] Priming read from SQS...')
    msg_ctr = 0 # keep a count of messages processed
    sqs_msgs = client['sqs']['handle'].receive_message(
        QueueUrl=queue_endpoint,
        AttributeNames=['All'],
        MaxNumberOfMessages=10,
        VisibilityTimeout=60
    )
    sqs_delete = []
    while 'Messages' in sqs_msgs:
        print('INFO [CNUM-' + str(cnum) + '] Processing ' + str(len(sqs_msgs['Messages'])) + ' messages')
        for message in sqs_msgs['Messages']:
            rc = message_handler(json.loads(message['Body']))
            # If we did not get a 0 return code let the record time out back
            # back into the queue
            if not rc:
                sqs_delete.append({'Id': message['MessageId'], 'ReceiptHandle':  message['ReceiptHandle']})
                msg_ctr += 1 # keep a count of messages processed

        if len(sqs_delete) > 0:
            # Delete the messages we just processed
            response = client['sqs']['handle'].delete_message_batch(
                QueueUrl=queue_endpoint,
                Entries=sqs_delete
            )
            if len(response['Successful']) < len(sqs_delete):
                print('ERROR[CNUM-' + str(cnum) + ']: processed ' + str(len(sqs_msgs)) + ' messages but only deleted ' + str(len(response['Successful'])) + ' messages')

        sqs_delete = [] # reset the list

        print('INFO [CNUM-' + str(cnum) + '] Reading from SQS...')
        sqs_msgs = client['sqs']['handle'].receive_message(
            QueueUrl=queue_endpoint,
            AttributeNames=['All'],
            MaxNumberOfMessages=10,
            VisibilityTimeout=60
        )

    print('INFO [CNUM-' + str(cnum) + '] Completed - ' + str(msg_ctr) + ' messages processed')

    if SEND_ANONYMOUS_USAGE_METRIC and msg_ctr > 0:
        send_anonymous_usage_metric({
            "Action": f"Num messages processed by CRRMonitor: {msg_ctr}"
        })

def send_anonymous_usage_metric(metric_data={}):
    try:
        if type(metric_data) is not dict or not dict:
            raise Exception('Invalid metric_data passed to send_anonymous_usage_metric')

        metric_endpoint = 'https://metrics.awssolutionsbuilder.com/generic'
        metric_payload = {
            "Solution": "SO0022",
            "UUID": ANONYMOUS_SOLUTION_ID,
            "Version": VERSION_ID,
            "Timestamp": str(datetime.utcnow()),
            "Data": metric_data
        }
        data = bytes(json.dumps(metric_payload), 'utf-8')
        headers = { "Content-Type": "application/json" }

        print(f"Sending anonymous usage metric: {str(metric_payload)}")

        req = urllib.request.Request(url=metric_endpoint, data=data, method='POST', headers=headers)
        with urllib.request.urlopen(req) as f:
            print(f"Anonymous usage metric send status: {f.status}")
    except Exception as e:
        # Log the exception but do not raise it again
        print(f'Exception while sending anonymous usage metric: {e}')

###### M A I N ######
client = connect_clients(client)
try:
    queue_endpoint = client['sqs']['handle'].get_queue_url(
        QueueName=queue
    )['QueueUrl']
except Exception as e:
    print(e)
    print('Could not get the url for ' + queue)
    raise e
