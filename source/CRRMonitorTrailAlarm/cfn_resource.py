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

import json
import logging
import sys
if sys.version_info.major == 3:
    from urllib.request import urlopen, Request, HTTPError, URLError
    from urllib.parse import urlencode
else:
    from urllib2 import urlopen, Request, HTTPError, URLError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SUCCESS = 'SUCCESS'
FAILED = 'FAILED'

"""
Event example
{
    "Status": SUCCESS | FAILED,
    "Reason: mandatory on failure
    "PhysicalResourceId": string,
    "StackId": event["StackId"],
    "RequestId": event["RequestId"],
    "LogicalResourceId": event["LogicalResourceId"],
    "Data": {}
}
"""

def wrap_user_handler(func, base_response=None):
    def wrapper_func(event, context):
        response = {
            "StackId": event["StackId"],
            "RequestId": event["RequestId"],
            "LogicalResourceId": event["LogicalResourceId"],
            "Status": SUCCESS,
        }
        if event.get("PhysicalResourceId", False):
            response["PhysicalResourceId"] = event["PhysicalResourceId"]

        if base_response is not None:
            response.update(base_response)

        logger.debug("Received %s request with event: %s" % (event['RequestType'], json.dumps(event)))

        try:
            response.update(func(event, context))
        except:
            logger.exception("Failed to execute resource function")
            response.update({
                "Status": FAILED,
                "Reason": "Exception was raised while handling custom resource"
            })

        serialized = json.dumps(response)
        logger.info("Responding to '%s' request with: %s" % (
            event['RequestType'], serialized))

        if sys.version_info.major == 3:
            req_data = serialized.encode('utf-8')
        else:
            req_data = serialized
        req = Request(
            event['ResponseURL'], data=req_data,
            headers={'Content-Length': len(req_data),
                     'Content-Type': ''}
        )
        req.get_method = lambda: 'PUT'

        try:
            urlopen(req)
            logger.debug("Request to CFN API succeeded, nothing to do here")
        except HTTPError as e:
            logger.error("Callback to CFN API failed with status %d" % e.code)
            logger.error("Response: %s" % e.reason)
        except URLError as e:
            logger.error("Failed to reach the server - %s" % e.reason)

    return wrapper_func

class Resource(object):
    _dispatch = None

    def __init__(self, wrapper=wrap_user_handler):
        self._dispatch = {}
        self._wrapper = wrapper

    def __call__(self, event, context):
        request = event['RequestType']
        logger.debug("Received {} type event. Full parameters: {}".format(request, json.dumps(event)))
        return self._dispatch.get(request, self._succeed())(event, context)

    def _succeed(self):
        @self._wrapper
        def success(event, context):
            return {
                'Status': SUCCESS,
                'PhysicalResourceId': event.get('PhysicalResourceId', 'mock-resource-id'),
                'Reason': 'Life is good, man',
                'Data': {},
            }
        return success

    def create(self, wraps):
        self._dispatch['Create'] = self._wrapper(wraps)
        return wraps

    def update(self, wraps):
        self._dispatch['Update'] = self._wrapper(wraps)
        return wraps

    def delete(self, wraps):
        self._dispatch['Delete'] = self._wrapper(wraps)
        return wraps
