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
# Description: "Copy the given keys from source to destination. Omit already existing keys."
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
import json


# Constants

DEBUG = False
THREAD_PARALLELISM = 10  # Empirical value for now. Should find good way to measure/auto-scale this.
METADATA_KEYS = [
    'CacheControl',
    'ContentDisposition',
    'ContentEncoding',
    'ContentLanguage',
    'ContentType',
    'Expires',
    'Metadata'
]


# Globals

logger = logging.getLogger()
if DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


# Utility functions

def collect_metadata(response):
    metadata = {}
    for key in METADATA_KEYS:
        if key in response:
            metadata[key] = response[key]
    metadata_json = json.dumps(metadata, sort_keys=True, default=str)
    return metadata_json


# Classes

class KeySynchronizer(Thread):
    def __init__(self, job_queue=None, source=None, destination=None, region=None):
        super(KeySynchronizer, self).__init__()
        self.job_queue = job_queue
        self.source = source
        self.destination = destination
        self.s3 = boto3.client('s3', region_name=region)

    def copy_redirect(self, key, target):
        logger.info(
            'Copying redirect: ' + key + ' from bucket: ' + self.source +
            ' to destination bucket: ' + self.destination
        )
        self.s3.put_object(
            Bucket=self.destination,
            Key=key,
            WebsiteRedirectLocation=target
        )

    def copy_object(self, key):
        logger.info(
            'Copying key: ' + key + ' from bucket: ' + self.source +
            ' to destination bucket: ' + self.destination
        )
        self.s3.copy_object(
            CopySource={
                'Bucket': self.source,
                'Key': key
            },
            Bucket=self.destination,
            Key=key
        )

    def run(self):
        while not self.job_queue.empty():
            try:
                key = self.job_queue.get(True, 1)
            except Empty:
                return

            source_response = self.s3.head_object(Bucket=self.source, Key=key)
            try:
                destination_response = self.s3.head_object(Bucket=self.destination, Key=key)
            except ClientError as e:
                if int(e.response['Error']['Code']) == 404:  # 404 = we need to copy this.
                    if 'WebsiteRedirectLocation' in source_response:
                        self.copy_redirect(key, source_response['WebsiteRedirectLocation'])
                    else:
                        self.copy_object(key)
                    continue
                else:  # All other return codes are unexpected.
                    raise e

            if 'WebsiteRedirectLocation' in source_response:
                if (
                    source_response['WebsiteRedirectLocation'] !=
                    destination_response.get('WebsiteRedirectLocation', None)
                ):
                    self.copy_redirect(key, source_response['WebsiteRedirectLocation'])
                continue

            source_etag = source_response.get('ETag', None)
            destination_etag = destination_response.get('ETag', None)
            if source_etag != destination_etag:
                self.copy_object(key)
                continue

            source_metadata = collect_metadata(source_response)
            destination_metadata = collect_metadata(destination_response)
            if source_metadata == destination_metadata:
                logger.info(
                    'Key: ' + key + ' from bucket: ' + self.source +
                    ' is already current in destination bucket: ' + self.destination
                )
                continue
            else:
                self.copy_object(key)


# Functions

def sync_keys(source=None, destination=None, region=None, keys=None):
    job_queue = Queue()
    worker_threads = []

    for i in range(THREAD_PARALLELISM):
        worker_threads.append(KeySynchronizer(
            job_queue=job_queue,
            source=source,
            destination=destination,
            region=region,
        ))

    for key in keys:
        logger.info('Queuing: ' + key + ' for synchronization.')
        job_queue.put(key)

    logger.info(
        'Starting ' + str(THREAD_PARALLELISM) + ' key synchronization processes for buckets: ' + source +
        ' and ' + destination + '.'
    )
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

    logger.info('Copying ' + str(len(keys)) + ' keys from bucket: ' + source + ' to bucket: ' + destination)

    sync_keys(source=source, destination=destination, keys=keys, region=region)

    return
