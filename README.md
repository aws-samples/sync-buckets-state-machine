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

## Prerequisites

You will need a system with Python 2.7 and virtualenv (https://virtualenv.pypa.io/en/stable/installation/) installed,
and an AWS account that is configured on your system to be ready to use with the AWS CLI.

(We won't use the AWS CLI but will use the AWS credentials stored in its configuration files.)

## How to install

      > sudo yum install -y gcc libffi-devel openssl-devel               # Make sure some prerequisites are installed.
      > virtualenv env                                                   # Create a Python virtual environment.
      > cd env; . ./bin/activate                                         # Activate the Python virtual environment.
      > git clone https://github.com/awslabs/sync-buckets-state-machine  # Clone the software from this Git repository.
      > cd sync-buckets-state-machine
      > pip install -r requirements.txt                                  # This will also install the "fab" utility from http://www.fabfile.org.
      > cp fabfile_config_template.py fabfile_config.py
      > vi fabfile_config.py                                             # Fill in your own values.
      > fab                                                              # Install everything into your AWS account.

## How to use

Start the Amazon Step Functions console in your chosen region and start a new execution with an input like:

```json
{
    "source": "your-source-bucket-name",
    "destination": "your-destination-bucket-name"
}
```

Optionally sync based on a prefix:

```json
{
    "source": "...",
    "destination": "...",
    "prefix": "images/"
}
```

Optionally sync after a specific key:

```json
{
    "source": "...",
    "destination": "...",
    "startAfter": "images/1000"
}
```

## How to uninstall   

This assumes that you're still working from the sync-buckets-state-machine that you installed into in the steps above.

      > fab delete                 # Delete the CloudFormation stack and its resources.
      > deactivate                 # Deactivate the Python virtual environment
      > cd ../..; /bin/rm -rf env  # Clean up.

## Files/directories

* *lambda_functions*: All AWS Lambda functions are stored here. They contain YAML front matter with their configuration.
* *state_machines*: All AWS Step Functions state machine definitions are stored here in YAML.
* *fabfile.py*: Python fabric file that builds a CloudFormation stack with all Lambda functions and their configuration.
  It extracts configuration information from each Lambda function source file's YAML front matter and uses it to
  generate AWS CloudFormation snippets for the AWS Serverless Application Model (SAM) to simplify deployment.
  It also creates an IAM Role resource in the CloudFormation template for the Step Functions state machine. After
  creating or updating the CloudFormation stack, it proceeds to create/update the Step Functions state machine, using
  a timestamp suffix to distinguish different state machine versions from each other.
* *README*: This file.
* *requirements.txt*: Python requirements for this project.

## Feedback

Please send feedback, suggestions, etc. to glez@amazon.de (Constantin Gonzalez)
