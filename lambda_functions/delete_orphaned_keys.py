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
# Description: "Delete keys present in the given destination bucket that are not present in the source bucket."
# MemorySize: 128
# Timeout: 300
# Policies:
#     - AmazonS3FullAccess
# ---
#
# Input event: A dict like:
# {
#     'source': 'source-bucket',
#     'sourceRegion': 'eu-west-1',
#     'destination': 'destination-bucket',
#     'destinationRegion': 'eu-west-1',
#     'keys': [ ... ]
# }
#

# Imports

import logging
import boto3
from threading import Thread
from botocore.exceptions import ClientError
from Queue import Queue, Empty


# Constants

DEBUG = False
THREAD_PARALLELISM = 10


# Globals

logger = logging.getLogger()
if DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


# Classes

class ObsoleteKeyDeleter(Thread):
    def __init__(self, job_queue=None, source=None, destination=None, region=None):
        super(ObsoleteKeyDeleter, self).__init__()
        self.job_queue = job_queue
        self.source = source
        self.destination = destination
        self.s3 = boto3.client('s3', region_name=region)

    def run(self):
        while not self.job_queue.empty():
            try:
                key = self.job_queue.get(True, 1)
            except Empty:
                return

            try:
                self.s3.head_object(Bucket=self.source, Key=key)
                logger.info('Key: ' + key + ' is present in source bucket, nothing to do.')
            except ClientError as e:
                if int(e.response['Error']['Code']) == 404:  # The key was not found.
                    logger.info('Key: ' + key + ' is not present in source bucket. Deleting orphaned key.')
                    self.s3.delete_object(Bucket=self.destination, Key=key)
                else:
                    raise e


# Functions

def delete_obsolete_keys(source=None, destination=None, region=None, keys=None):
    job_queue = Queue()
    worker_threads = []

    for i in range(THREAD_PARALLELISM):
        worker_threads.append(ObsoleteKeyDeleter(
            job_queue=job_queue,
            source=source,
            destination=destination,
            region=region,
        ))

    for key in keys:
        logger.info('Queuing: ' + key + ' for orphan detection.')
        job_queue.put(key)

    logger.info('Starting orphan detection for buckets: ' + source + ' and ' + destination + '.')
    for t in worker_threads:
        t.start()

    for t in worker_threads:
        t.join()


def handler(event, context):
    assert(isinstance(event, dict))

    source = event['source']
    destination = event['destination']
    keys = event['listResult']['keys']

    function_region = context.invoked_function_arn.split(':')[3]
    region = event.get('sourceRegion', function_region)

    logger.info('Synchronizing ' + str(len(keys)) + ' between bucket: ' + source + ' and: ' + destination)

    delete_obsolete_keys(source=source, destination=destination, keys=keys, region=region)

    return
