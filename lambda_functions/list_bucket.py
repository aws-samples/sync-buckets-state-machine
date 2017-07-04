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
# Description: "Prepare for next copy cycle: List source bucket contents, updates token for longer lists if necessary."
# MemorySize: 128
# Timeout: 60
# Policies:
#     - AmazonS3ReadOnlyAccess
# ---
#
# Input event: A string with the source bucket name and optional region and token (for s3.list_objects_v2()).
#

# Imports

import logging
import boto3
import json


# Constants

DEBUG = False
MAX_KEYS = 1024  # Should be a power of two since it may get divided by two a couple of times.
MAX_DATA_SIZE = 32000  # Max. result size: https://docs.aws.amazon.com/step-functions/latest/dg/service-limits.html
SAFETY_MARGIN = 10.0  # Percent
MAX_RESULT_LENGTH = int(MAX_DATA_SIZE * (1.0 - (SAFETY_MARGIN / 100.0)))
PREFIX = '' # Copy objects based on a provided prefix e.g. '/images/'


# Globals

logger = logging.getLogger()
if DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


# Functions

def handler(event, context):
    assert(isinstance(event, dict))

    bucket_to_list = event['listBucket']
    bucket = event[bucket_to_list]

    function_region = context.invoked_function_arn.split(':')[3]
    region = event.get('sourceRegion', function_region)

    token = event.get('listResult', {}).get('token', '')
    max_keys = event.get('maxKeys', MAX_KEYS)
    prefix = event.get('prefix', PREFIX)

    args = {
        'Bucket': bucket,
        'MaxKeys': max_keys,
        'Prefix': prefix
    }

    result = {}
    s3 = boto3.client('s3', region_name=region)

    while True:
        logger_string = 'Listing contents of bucket: ' + bucket + ' in: ' + region + ' ('
        if token is not None and token != '':
            logger_string += 'continuation token: ' + token + ', '
            args['ContinuationToken'] = token
        logger_string += 'may_keys: ' + str(max_keys) + ')'

        response = s3.list_objects_v2(**args)

        keys = [k['Key'] for k in response.get('Contents', [])]
        logger.info('Got ' + str(len(keys)) + ' result keys.')

        result['keys'] = keys
        result['token'] = response.get('NextContinuationToken', '')
        result_length = len(json.dumps(result))
        if result_length <= MAX_RESULT_LENGTH:
            return result
        else:
            # Try again with a smaller may_keys size.
            logger.warning(
                'Result size: ' + str(result_length) + ' is larger than maximum of: ' + str(MAX_RESULT_LENGTH) + '. '
            )

            max_keys = int(len(keys) / 2)  # ask for half the number of keys we got.
            if max_keys == 0:
                raise Exception('Something is wrong: Downsized max_keys all the way to 0 ...')
            args['MaxKeys'] = max_keys
            logger.info('Trying again with max_keys value: ' + str(max_keys))
