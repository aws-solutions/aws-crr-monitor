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

# solution-helper.py
# This code generates a uuid using the uuid random tool
import json 
import uuid 
import urllib.request
 
def send(event, context, responseStatus, responseData, physicalResourceId=None, noEcho=False): 
    try: 
        responseUrl = event.get('ResponseURL') 
         
        responseBody = { 
            "Status":responseStatus, 
            "Reason": "See the details in CloudWatch Log Stream: " + context.log_stream_name, 
            "PhysicalResourceId": physicalResourceId or context.log_stream_name, 
            "StackId": event.get('StackId'), 
            "RequestId":event.get('RequestId'), 
            "LogicalResourceId": event.get('LogicalResourceId'), 
            "NoEcho": noEcho, 
            "Data": responseData 
        } 
         
        data = bytes(json.dumps(responseBody), 'utf-8')
         
        headers = { 
            'content-type' : '', 
            'content-length' : str(len(data)) 
        } 
 
        req = urllib.request.Request(url=responseUrl, data=data, method='PUT', headers=headers)
        with urllib.request.urlopen(req) as f:
            print(f"CFN Status: {f.status}")
    except Exception as e: 
        raise(e) 

def lambda_handler(event, context): 
    try: 
        request = event.get('RequestType') 
        responseData = {} 
 
        if request == 'Create': 
            responseData = {'UUID':str(uuid.uuid4())} 
 
        send(event, context, 'SUCCESS', responseData) 
    except Exception as e: 
        print('Exception: {}'.format(e))
        send(event, context, 'FAILED', {}, context.log_stream_name)
