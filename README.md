# SyncBucketsStateMachine

## Legal notice

Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at

http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file.
This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.

## Introduction

This AWS Step Functions (SFN) state machine is designed to one-way synchronize an Amazon S3 source bucket
into an Amazon S3 destination bucket as follows:

* All files in the source bucket that are not present in the destination bucket or don't match their destination ETag
  are copied from source to destination.
* All files in the destination bucket that are not present in the source bucket are deleted.

This code is purely meant for illustration/demonstration purposes, please use it at your own risk. Although it has been
developed carefully and with the best intentions in mind, there is no guarantee and there may be bugs. It **will copy
real files** and it **will delete real files** in your Amazon S3 buckets in case it deems so necessary. To avoid any
damage, please use it only with Amazon S3 buckets that contain purely test and demonstration data.

## How to use

1. git clone https://github.com/awslabs/sync-buckets-state-machine
2. pip install -r requirements.txt  # This should also install the "fab" command-line tool from http://fabfile.org/
3. cp fabfile_config_template.py fabfile_config.py
4. vi fabfile_config.py # Fill in your own values.
5. fab
6. Start the Amazon Step Functions console in your chosen region and start a new execution with an input like:
   >    {
   >        "source": "your-source-bucket-name",
   >        "destination:" "your-destination-bucket-name"
   >    }
   
## Files/directories

* *lambda_functions*: All AWS Lambda functions are stored here. They contain YAML front matter with their configuration.
* *state_machines*: All AWS Step Functions state machine definitions are stored here in YAML.
* *fabfile.py*: Python fabric file that builds a CloudFormation stack with all Lambda functions and their configuration.
  It extracts configuration information from each Lambda function source file's YAML front matter and uses it to
  generate AWS CloudFormation snippets for the AWS Serverless Application Model (SAM) to simplify deployment.
  It also creates resources in CloudFormation for an IAM Role and for the Step Functions state machine.
  After creating the CloudFormation template, the fabfile will create or update the corresponding stack in your account.
* *README.md*: This file.
* *requirements.txt*: Python requirements for this project.
   
## Feedback

Please send feedback, suggestions, etc. to Constantin Gonzalez, glez@amazon.de
