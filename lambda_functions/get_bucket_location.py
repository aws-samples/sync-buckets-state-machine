# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

#
# YAML front matter with parameters for deployment as a Lambda function.
#
# ---
# Description: "Get the location of the given AWS bucket, return its region name."
# MemorySize: 128
# Timeout: 10
# Policies:
#     - AmazonS3ReadOnlyAccess
# ---
#
# Input event: A string with the bucket name to query the region name for.
#

# Imports

import logging
import boto3


# Constants

DEBUG = False


# Globals

logger = logging.getLogger()
if DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


# Functions

def handler(event, context):
    if isinstance(event, (str, unicode)):
        bucket = event
    else:  # Find the first attribute in the dict that contains somehow the string 'bucket'.
        assert(isinstance(event, dict))

        bucket_keys = [i for i in event.keys() if 'bucket' in i.lower()]
        if len(bucket_keys) > 0:
            bucket = event[bucket_keys[0]]
        else:
            bucket = event[event.keys()[0]]  # Give up and just go for the first key.

    assert(bucket is not None and isinstance(bucket, (str, unicode)) and bucket != '')
    function_region = context.invoked_function_arn.split(':')[3]

    logger.info('Looking up bucket location for bucket: ' + bucket)

    s3 = boto3.client('s3', region_name=function_region)
    response = s3.get_bucket_location(Bucket=bucket)
    location_constraint = response.get('LocationConstraint', None)
    if location_constraint is None:
        return 'us-east-1'
    elif location_constraint == 'EU':
        return 'eu-west-1'
    else:
        return location_constraint
