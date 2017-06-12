from __future__ import print_function

import boto3
import os
from datetime import datetime,timedelta

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
#######################################################################
# Check for incomplete transfers that started more than an hour ago.
# - If it no longer exists, discard it
# - If it failed, increment the CloudWatch metric
# - If it completed, update the DDB fields. Note that currently we
#   don't do anything about this - should never happen. Future enhancement
#######################################################################

#
# Define the DynamoDB table to be used to track replication status.
#   It must be in the same region as this Lambda and should already
#   exist. It is created by the CloudFormation template.
#
ddbtable = getparm('appname', 'CRRMonitor')
stattable = ddbtable + 'Statistics'
timefmt = '%Y-%m-%dT%H:%M:%SZ'
roundTo = getparm('roundTo', 300) # 5 minute buckets for CW metrics
purge_thresh = getparm('purge_thresh', 24) # threshold in hours
client={
    's3': { 'service': 's3' },
    'ddb': { 'service': 'dynamodb'}
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
                clients_to_connect[c]['handle']=boto3.client(clients_to_connect[c]['service'], region_name=clients_to_connect[c]['region'])
            else:
                clients_to_connect[c]['handle']=boto3.client(clients_to_connect[c]['service'])
        except Exception as e:
            print(e)
            print('Error connecting to ' + clients_to_connect[c]['service'])
            raise e
    return clients_to_connect

# =====================================================================
# lambda_handler
# --------------
# Look for failed replication and other anomalies.
# =====================================================================
def lambda_handler(event, context):
    # -----------------------------------------------------------------
    # purge_item - removes old items
    #
    def purge_item(itemkey):
        print('Purge ETag: ' + itemkey)
        try:
            client['ddb']['handle'].delete_item(
                TableName=ddbtable,
                Key={
                    'ETag': {
                        'S': itemkey
                    }
                }
            )
        except Exception as e:
            print(e)
            print('Error purging ' + itemkey + ' from ' + ddbtable)
            raise e
    # -----------------------------------------------------------------
    # log_statistics
    #
    def log_statistics(Src,Dst,Tstamp,Size,ET,roundTo):
        # -------------------------------------------------------------
        # Derive the statistic bucket from source/dest and time bucket
        # (5 minute rolling window)
        #
        statbucket=Src + ':' + Dst
        ts = datetime.strptime(Tstamp, timefmt)
        secs = (ts.replace(tzinfo=None) - ts.min).seconds
        rounding = (secs+roundTo/2) // roundTo * roundTo
        ts = ts + timedelta(0,rounding-secs,-ts.microsecond)
        statbucket += ':' + datetime.strftime(ts, timefmt)
        # -------------------------------------------------------------
        # Init a dict to use to hold our attrs for DDB
        stat_exp_attrs = {}
        # -------------------------------------------------------------
        # Build the DDB UpdateExpression
        stat_update_exp = 'SET timebucket = :t, source_bucket = :o, dest_bucket = :r ADD objects :a, size :c, elapsed :d'
        # -------------------------------------------------------------
        # push the first attr: s3Object
        stat_exp_attrs[':a'] = { 'N': '1' }
        stat_exp_attrs[':c'] = { 'N': Size }
        stat_exp_attrs[':d'] = { 'N': ET }
        stat_exp_attrs[':t'] = { 'S': datetime.strftime(ts, timefmt) }
        stat_exp_attrs[':o'] = { 'S': Src }
        stat_exp_attrs[':r'] = { 'S': Dst }
        #print('s3Object: ' + key)
        try:
            client['ddb']['handle'].update_item(
                TableName = stattable,
                Key = { 'OriginReplicaBucket': { 'S': statbucket } },
                UpdateExpression = stat_update_exp,
                ExpressionAttributeValues = stat_exp_attrs)
        except Exception as e:
            print(e)
            print('Table ' + stattable + ' update failed')
            raise e

    # -----------------------------------------------------------------
    # process_items - check each item returned by the scan
    #
    def process_items(items):
        for i in items:

            # Call head-object to check replication status
            try:
                response = client['s3']['handle'].head_object(
                    Bucket=i['s3Origin']['S'],
                    Key=i['s3Object']['S'])
            except Exception as e:
                print('Item no longer exists - purging: ' + i['ETag']['S'])
                purge_item(i['ETag']['S'])
                continue
            # Init a dict to use to hold our attrs for DDB
            ddb_exp_attrs = {}
            # Build th e DDB UpdateExpression
            ddb_update_exp = 'set s3Object = :a'
            # push the first attr: s3Object
            ddb_exp_attrs[':a'] = { 'S': i['s3Object']['S'] }

            # Object still exists
            headers = response['ResponseMetadata']['HTTPHeaders']

            lastmod = datetime.strftime(response['LastModified'], timefmt)

            if headers['x-amz-replication-status'] == 'COMPLETED':
                print('Completed transfer found: ' + i['ETag']['S'])
                ddb_update_exp += ', replication_status = :b'
                ddb_exp_attrs[':b'] = { 'S': 'COMPLETED' }
                #print(response)
            elif headers['x-amz-replication-status'] == 'FAILED':
                ddb_update_exp += ', replication_status = :b'
                ddb_exp_attrs[':b'] = { 'S': 'FAILED' }
                log_statistics(i['s3Origin']['S'],'FAILED',i['start_datetime']['S'],'0','1',300)

            # Update the record in the DDB table
            try:
                client['ddb']['handle'].update_item(
                    TableName = ddbtable,
                    Key = { 'ETag': i['ETag'] },
                    UpdateExpression = ddb_update_exp,
                    ExpressionAttributeValues = ddb_exp_attrs)
            except Exception as e:
                print(e)
                print('Table ' + ddbtable + ' update failed')
                raise e

    # -----------------------------------------------------------------
    # check_incompletes
    #
    print('Checking for incomplete transfers')
    check = datetime.utcnow() - timedelta(hours=1) # datetime object
    checkstr= check.strftime(timefmt) # string object
    # Set scan filter attrs
    eav = {
        ":check": { "S": checkstr },
        ":completed": { "S": "COMPLETED" }
    }

    print('Reading from ' + ddbtable)
    try:
        response = client['ddb']['handle'].scan(
            TableName=ddbtable,
            ExpressionAttributeValues=eav,
            FilterExpression="replication_status <> :completed and start_datetime < :check",
            Limit=1000
            )
    except Exception as e:
        print(e)
        print('Table ' + ddbtable + ' scan failed')
        raise e

    print('Checking for incomplete items from ' + ddbtable)
    process_items(response['Items'])


    while 'LastEvaluatedKey' in response:
        response = client['ddb']['handle'].scan(
            TableName=ddbtable,
            FilterExpression="replication_status <> :completed and start_datetime < :check",
            ExpressionAttributeValues=eav,
            ExclusiveStartKey=response['LastEvaluatedKey'],
            Limit=1000
            )

        process_items(response['Items'])

###### M A I N ######
client = connect_clients(client)
