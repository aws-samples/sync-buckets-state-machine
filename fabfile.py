#!/usr/bin/python
# coding: utf-8

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
# Fabfile for running state machines that synchronize Amazon S3 bucket contents.
#

# Imports

# All user configurable options are kept in the fabfile_config.py file that is imported here.
from fabfile_config import *

from fabric.api import task
import logging
import json
import boto3
from botocore.exceptions import ClientError
import time
from io import BytesIO
from zipfile import ZipFile, ZIP_DEFLATED
import os
from datetime import datetime
import yaml
from hashlib import md5

# Constants

USER_HASH = md5(USER_EMAIL).hexdigest()
CONSTANTS_TO_ENVIRONMENT = [
    'AWS_DEFAULT_REGION',
    'AWS_DEFAULT_PROFILE'
]
for c in CONSTANTS_TO_ENVIRONMENT:
    if c in globals():
        os.environ[c] = globals()[c]

SLEEP_TIME = 5  # seconds

# Lambda
LAMBDA_FUNCTION_DEPLOYMENT_BUCKET = USER_HASH + '-' + AWS_DEFAULT_REGION + '-lambda-deployment'
LAMBDA_FUNCTION_CODE_URI_PREFIX = 's3://' + LAMBDA_FUNCTION_DEPLOYMENT_BUCKET + '/'
LAMBDA_FUNCTION_DIRECTORY = 'lambda_functions'
LAMBDA_DEFAULT_RUNTIME = 'python2.7'
LAMBDA_DEFAULT_DESCRIPTION = 'An AWS Lambda function.'
LAMBDA_DEFAULT_MEMORY_SIZE = 128  # MB
LAMBDA_DEFAULT_TIMEOUT = 30  # seconds
LAMBDA_DEFAULT_POLICY = 'AWSLambdaBasicExecutionRole'
LAMBDA_DEFAULT_PARAMETERS = {
    'Description': LAMBDA_DEFAULT_DESCRIPTION,
    'Runtime': LAMBDA_DEFAULT_RUNTIME,
    'MemorySize': LAMBDA_DEFAULT_MEMORY_SIZE,
    'Timeout': LAMBDA_DEFAULT_TIMEOUT,
    'Policies': [LAMBDA_DEFAULT_POLICY],
}


# Step Functions

STATE_MACHINE_DIRECTORY = 'state_machines'

# Use the first state machine definition name as the overall APP_NAME. Feel free to override in fabfile_config.py
if os.path.exists(STATE_MACHINE_DIRECTORY):
    MAIN_APP_DIRECTORY = STATE_MACHINE_DIRECTORY
else:
    MAIN_APP_DIRECTORY = LAMBDA_FUNCTION_DIRECTORY

APP_NAME = globals().get(
    'APP_NAME',
    str([os.path.splitext(sm)[0] for sm in os.listdir(MAIN_APP_DIRECTORY)][0])  # type check wants str().
)
APP_NAME = APP_NAME.replace('_', '-')  # Make Cloudformation naming conventions happy.

STATE_MACHINE_ROLE_POSTFIX = 'Role'
STATE_MACHINE_TRUSTED_ENTITY = 'states.' + AWS_DEFAULT_REGION + '.amazonaws.com'
STATE_MACHINE_DEFAULT_POLICIES = ['AWSLambdaRole']


# CloudFormation

CFN_STACK_NAME = APP_NAME + '-stack'
CFN_STACK_CHANGE_SET_NAME = APP_NAME + '-change-set'

CFN_TEMPLATE = {
    'AWSTemplateFormatVersion': '2010-09-09',
    'Transform': 'AWS::Serverless-2016-10-31',
    'Resources': {},
    'Outputs': {}
}

CFN_TEMPLATE_LAMBDA_FUNCTION = {
    'Type': 'AWS::Serverless::Function',
    'Properties': LAMBDA_DEFAULT_PARAMETERS
}

CFN_TEMPLATE_ROLE = {
    'Type': 'AWS::IAM::Role',
    'Properties': {
        'AssumeRolePolicyDocument': {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Sid': 'TrustPolicy',
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': ''
                    },
                    'Action': 'sts:AssumeRole'
                }
            ]
        },
        'ManagedPolicyArns': [],
    }
}

CFN_TEMPLATE_OUTPUT = {
    'Description': '',
    'Value': {
        'Fn::GetAtt': [
            '',
            'Arn'
        ]
    }
}

CFN_TEMPLATE_STATE_MACHINE = {
   'Type': 'AWS::StepFunctions::StateMachine',
   'Properties': {
      'DefinitionString': None,
      'RoleArn': None
    }
}


# Globals

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.WARNING)

lambda_functions = {}
state_machines = {}


# Functions

def dict_to_normalized_json(d):
    return json.dumps(d, sort_keys=True, indent=4)


def compare_json_or_dicts(o1, o2):
    if isinstance(o1, basestring):
        json1 = dict_to_normalized_json(json.loads(o1))
    else:
        json1 = dict_to_normalized_json(o1)

    if isinstance(o2, basestring):
        json2 = dict_to_normalized_json(json.loads(o2))
    else:
        json2 = dict_to_normalized_json(o2)

    return json1 == json2


def to_camel_case(s):
    components = s.replace('-', '_').split('_')
    return ''.join([i[0].upper() + i[1:].lower() for i in components])


# S3

def check_bucket(bucket):
    s3 = boto3.client('s3', region_name=AWS_DEFAULT_REGION)

    print('Checking bucket: ' + bucket)
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        print('Creating bucket: ' + bucket)
        args = {
            'Bucket': bucket
        }
        if AWS_DEFAULT_REGION != 'us-east-1':
            args['CreateBucketConfiguration'] = {
                'LocationConstraint': AWS_DEFAULT_REGION
            }
        s3.create_bucket(**args)
        waiter = s3.get_waiter('bucket_exists')
        waiter.wait(Bucket=bucket)


def upload_object_to_s3(bucket, key, o):
    s3 = boto3.client('s3', region_name=AWS_DEFAULT_REGION)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=o
    )


def get_timestamp_from_s3_object(bucket, key):
    s3 = boto3.client('s3', region_name=AWS_DEFAULT_REGION)

    try:
        response = s3.get_object(
            Bucket=bucket,
            Key=key
        )
        timestamp = response['LastModified']  # We assume this is UTC.
    except ClientError:
        timestamp = datetime(1970, 1, 1, tzinfo=None)

    return (timestamp.replace(tzinfo=None) - datetime(1970, 1, 1, tzinfo=None)).total_seconds()


# IAM

def get_arn_from_policy_name(policy_name):
    iam = boto3.client('iam', region_name=AWS_DEFAULT_REGION)

    args = {
        'Scope': 'All'
    }
    while True:
        response = iam.list_policies(**args)
        for p in response['Policies']:
            if p['PolicyName'] == policy_name:
                return p['Arn']
        if response['IsTruncated']:
            args['Marker'] = response['Marker']
        else:
            return None


# Lambda

def populate_lambda_functions_dict():
    global lambda_functions

    if not os.path.exists(LAMBDA_FUNCTION_DIRECTORY):  # nothing to do.
        return

    for file_name in [
        i for i in os.listdir(LAMBDA_FUNCTION_DIRECTORY)
        if os.path.isfile(os.path.join(LAMBDA_FUNCTION_DIRECTORY, i)) and i.endswith('.py')
    ]:
        lambda_function_name = os.path.splitext(file_name)[0]
        lambda_function_parameters = json.loads(json.dumps(LAMBDA_DEFAULT_PARAMETERS))

        with open(os.path.join(LAMBDA_FUNCTION_DIRECTORY, file_name)) as f:
            yaml_lines = None
            while True:
                line = f.readline()
                if line == '':
                    break
                if line.startswith('#'):
                    line = line[2:]  # Cut comment and one space after it.
                    if yaml_lines is None:
                        if line.strip() == '---':
                            yaml_lines = []
                    else:
                        if line.strip() == '---':
                            break
                        else:
                            yaml_lines.append(line)

        if len(yaml_lines) > 0:
            yaml_dict = yaml.load(''.join(yaml_lines))
            lambda_function_parameters.update(yaml_dict)

        lambda_functions[lambda_function_name] = lambda_function_parameters


def generate_code_uri_for_lambda_function(lambda_function_name):
    return LAMBDA_FUNCTION_CODE_URI_PREFIX + lambda_function_name + '_' + str(int(time.time())) + '.zip'


def find_latest_code_uri_for_lambda_function(lambda_function_name):
    s3 = boto3.client('s3', region_name=AWS_DEFAULT_REGION)

    response = s3.list_objects_v2(Bucket=LAMBDA_FUNCTION_DEPLOYMENT_BUCKET, Prefix=lambda_function_name + '_')
    if response['KeyCount'] == 0:
        return None
    else:
        sorted_result = sorted(response['Contents'], key=lambda k: k['LastModified'])
        latest_key = sorted_result[-1]['Key']
        return LAMBDA_FUNCTION_CODE_URI_PREFIX + latest_key


def generate_lambda_function_cfn_template(lambda_function_name):
    lambda_function_template = json.loads(json.dumps(CFN_TEMPLATE_LAMBDA_FUNCTION))  # implements deep copy.

    properties = lambda_function_template['Properties']
    lambda_function_definition = lambda_functions[lambda_function_name]

    handler = lambda_function_name + '.handler'
    properties['Handler'] = handler

    code_uri = find_latest_code_uri_for_lambda_function(lambda_function_name)
    properties['CodeUri'] = code_uri

    # Overwrite the CloudFormation properties with selected properties from the function definition.
    for key in LAMBDA_DEFAULT_PARAMETERS.keys():
        if key in lambda_function_definition and lambda_function_definition[key] is not None:
            properties[key] = lambda_function_definition[key]

    # Make sure Policies make sense.
    if 'Policies' not in properties or properties['Policies'] is None:
        properties['Policies'] = [LAMBDA_DEFAULT_POLICY]
    if LAMBDA_DEFAULT_POLICY not in properties['Policies']:
        properties['Policies'].append(LAMBDA_DEFAULT_POLICY)

    logical_name = to_camel_case(lambda_function_name)
    output_template = generate_cfn_output_template(logical_name)

    result = {
        'Resources': {
            logical_name: lambda_function_template
        },
        'Outputs': {
            logical_name + 'Output': output_template
        }
    }
    return result


def create_lambda_deployment_package(lambda_function_name):
    print('Creating Lambda deployment package for: ' + lambda_function_name)

    zip_file = BytesIO()

    lambda_function_file_name = lambda_function_name + '.py'
    with ZipFile(zip_file, 'w', ZIP_DEFLATED) as z:
        print('Adding: ' + lambda_function_file_name + ' to ZIP archive.')
        z.write(os.path.join(LAMBDA_FUNCTION_DIRECTORY, lambda_function_file_name), lambda_function_file_name)

    return zip_file.getvalue()


def get_lambda_function_info(lambda_function_name):
    lambda_region = lambda_functions[lambda_function_name].get('region', AWS_DEFAULT_REGION)
    lambda_client = boto3.client('lambda', region_name=lambda_region)

    # noinspection PyBroadException
    try:
        response = lambda_client.get_function(FunctionName=lambda_function_name)
    except:
        response = None

    return response


def update_lambda_function_package(lambda_function_name):
    lambda_function_path = os.path.join(LAMBDA_FUNCTION_DIRECTORY, lambda_function_name + '.py')
    latest_code_uri = find_latest_code_uri_for_lambda_function(lambda_function_name)
    if latest_code_uri is not None:
        latest_code_key = latest_code_uri.split('/')[-1]
        latest_code_uri_timestamp = get_timestamp_from_s3_object(LAMBDA_FUNCTION_DEPLOYMENT_BUCKET, latest_code_key)
        local_code_timestamp = os.path.getmtime(lambda_function_path)

        if local_code_timestamp < latest_code_uri_timestamp:
            print('Lambda function deployment package for: ' + lambda_function_name + ' on S3 is current.')
            return

    new_code_uri = generate_code_uri_for_lambda_function(lambda_function_name)
    _, _, _, new_code_key = new_code_uri.split('/')

    print('Creating Lambda function deployment package.')
    lambda_function_deployment_package = create_lambda_deployment_package(lambda_function_name)
    print('Uploading Lambda function deployment package as: ' + new_code_key)
    upload_object_to_s3(LAMBDA_FUNCTION_DEPLOYMENT_BUCKET, new_code_key, lambda_function_deployment_package)


def update_lambda_function_packages():
    for lambda_function_name in lambda_functions.keys():
        update_lambda_function_package(lambda_function_name)


# Step Functions

def populate_state_machines_dict():
    global state_machines

    if not os.path.exists(STATE_MACHINE_DIRECTORY):
        return

    for file_name in [
        i for i in os.listdir(STATE_MACHINE_DIRECTORY)
        if os.path.isfile(os.path.join(STATE_MACHINE_DIRECTORY, i)) and i.endswith('.yaml')
    ]:
        state_machine_name = os.path.splitext(file_name)[0]
        with open(os.path.join(STATE_MACHINE_DIRECTORY, file_name)) as f:
            state_machine_dict = yaml.load(f)

        state_machines[state_machine_name] = state_machine_dict


def generate_state_machine_cfn_template(state_machine_name):
    result_template = {
        'Resources': {},
        'Outputs': {}
    }

    state_machine_logical_name = to_camel_case(state_machine_name)

    state_machine_role_name = state_machine_logical_name + STATE_MACHINE_ROLE_POSTFIX
    state_machine_role_template = generate_role_cfn_template(
        STATE_MACHINE_DEFAULT_POLICIES, STATE_MACHINE_TRUSTED_ENTITY
    )
    result_template['Resources'][state_machine_role_name] = state_machine_role_template

    state_machine_definition = state_machines[state_machine_name]
    state_machine_json = json.dumps(state_machine_definition, indent=4)
    state_machine_json_lines = state_machine_json.splitlines()

    definition_lines = []
    for line in state_machine_json_lines:
        if line.lstrip().startswith('"Resource":'):
            prefix, resource = line.split(':', 1)
            resource_name = resource.strip(' ",')
            if resource_name in lambda_functions.keys():
                line = {
                    'Fn::Join': [
                        '',
                        [
                            prefix,
                            ': "',
                            {
                                'Fn::GetAtt': [
                                    to_camel_case(resource_name),
                                    'Arn'
                                ]
                            },
                            '",'
                        ]
                    ]
                }
        definition_lines.append(line)

    definition_string_value = {
        'Fn::Join': [
            '\n',
            definition_lines
        ]
    }

    state_machine_template = json.loads(json.dumps(CFN_TEMPLATE_STATE_MACHINE))
    state_machine_template['Properties']['DefinitionString'] = definition_string_value
    state_machine_template['Properties']['RoleArn'] = {
        'Fn::GetAtt': [
            state_machine_logical_name + 'Role',
            'Arn'
        ]
    }

    result_template['Resources'][state_machine_logical_name] = state_machine_template

    output_name = state_machine_logical_name + 'Name'
    output_template = json.loads(json.dumps(CFN_TEMPLATE_OUTPUT))
    output_template['Description'] = 'Name for State Machine: ' + state_machine_logical_name
    output_template['Value']['Fn::GetAtt'][0] = state_machine_logical_name
    output_template['Value']['Fn::GetAtt'][1] = 'Name'

    result_template['Outputs'][output_name] = output_template

    return result_template


# CloudFormation

def generate_cfn_output_template(resource_name):
    result = json.loads(json.dumps(CFN_TEMPLATE_OUTPUT))
    result['Description'] = resource_name + ' ARN'
    result['Value']['Fn::GetAtt'][0] = resource_name

    return result


def generate_role_cfn_template(policies, trusted_entities):
    result = json.loads(json.dumps(CFN_TEMPLATE_ROLE))
    policy_arns = [get_arn_from_policy_name(p) for p in policies]

    result['Properties']['AssumeRolePolicyDocument']['Statement'][0]['Principal']['Service'] = trusted_entities
    result['Properties']['ManagedPolicyArns'] = policy_arns

    return result


def combine_templates(t1, t2):  # Combines t2 into t1, modifies t1.
    for i in t2.keys():
        if i in t1:
            t1[i].update(json.loads(json.dumps(t2[i])))  # deep copy.
        else:
            t1[i] = json.loads(json.dumps(t2[i]))
    return t1


def generate_cfn_template():
    result = json.loads(json.dumps(CFN_TEMPLATE))

    for lambda_function_name in lambda_functions.keys():
        print('Generating CloudFormation template for Lambda function: ' + lambda_function_name)
        lambda_function_template = generate_lambda_function_cfn_template(lambda_function_name)
        combine_templates(result, lambda_function_template)

    for state_machine_name in state_machines.keys():
        print('Generating CloudFormation template for state machine: ' + state_machine_name)
        state_machine_template = generate_state_machine_cfn_template(state_machine_name)
        combine_templates(result, state_machine_template)

    return result


def get_cfn_stack_info():
    cfn = boto3.client('cloudformation', region_name=AWS_DEFAULT_REGION)

    try:
        result = cfn.describe_stacks(StackName=CFN_STACK_NAME)
        return result['Stacks'][0]
    except ClientError:
        result = None

    return result


def create_cfn_change_set():
    stack_info = get_cfn_stack_info()
    print('Generating CloudFormation template.')
    template = json.dumps(generate_cfn_template(), sort_keys=True, indent=4)

    cfn = boto3.client('cloudformation', region_name=AWS_DEFAULT_REGION)
    args = {
        'StackName': CFN_STACK_NAME,
        'TemplateBody': template,
        'Capabilities': ['CAPABILITY_NAMED_IAM'],
        'ChangeSetName': CFN_STACK_CHANGE_SET_NAME,
        'ChangeSetType': 'UPDATE'
    }
    if stack_info is None or stack_info['StackStatus'] == 'REVIEW_IN_PROGRESS':
        print('CloudFormation stack: ' + CFN_STACK_NAME + ' does not exist. Creating change set for a new stack.')
        args['ChangeSetType'] = 'CREATE'
    elif stack_info['StackStatus'] == 'ROLLBACK_COMPLETE':
        print('CloudFormation stack: ' + CFN_STACK_NAME + ' is in ROLLBACK_COMPLETE state.')
        print('Deleting stack...')
        cfn.delete_stack(StackName=CFN_STACK_NAME)
        while True:
            print('Waiting for stack delete to complete.')
            stack_info = get_cfn_stack_info()
            if stack_info is None or stack_info['StackStatus'] == 'DELETE_COMPLETE':
                break
            time.sleep(SLEEP_TIME)
        args['ChangeSetType'] = 'CREATE'
    else:
        print(
            'CloudFormation stack: ' + CFN_STACK_NAME + ' exists already. Creating change set for updating the stack.'
        )

    try:
        cfn.delete_change_set(
            ChangeSetName=CFN_STACK_CHANGE_SET_NAME,
            StackName=CFN_STACK_NAME
        )
        while True:
            response = cfn.describe_change_set(ChangeSetName=CFN_STACK_CHANGE_SET_NAME, StackName=CFN_STACK_NAME)
            status = response['Status']
            print('Status: ' + status)
            if status == 'FAILED':
                exit(1)
            elif status.endswith('COMPLETED'):
                break
    except ClientError:
        pass

    response = cfn.create_change_set(**args)
    change_set_id = response['Id']

    print('Waiting for CloudFormation change set creation to complete...')
    while True:
        time.sleep(SLEEP_TIME)
        response = cfn.describe_change_set(ChangeSetName=change_set_id)
        status = response['Status']
        if status == 'FAILED':
            reason = response['StatusReason']
            if reason == 'No updates are to be performed.':
                print('No changes to the CloudFormation stack necessary.')
                return None
            else:
                print('Reason: ' + reason)
                exit(1)
        print('Status: ' + status)
        if status.endswith('COMPLETE'):
            return change_set_id


def execute_cfn_change_set(change_set_id):
    cfn = boto3.client('cloudformation', region_name=AWS_DEFAULT_REGION)

    print('Executing CloudFormation change set...')
    cfn.execute_change_set(ChangeSetName=change_set_id)

    while True:
        response = get_cfn_stack_info()
        if response is None:
            status = 'UNKNOWN'
        else:
            status = response.get('StackStatus', 'UNKNOWN')

        print('Status: ' + status)
        if 'StatusReason' in response:
            print('Reason: ' + response['StatusReason'])
        if status.endswith('FAILED') or status == 'ROLLBACK_COMPLETE':
            exit(1)
        elif status == 'UNKNOWN':
            print('Stack info:\n' + json.dumps(response, sort_keys=True, indent=4, default=str))
        elif status.endswith('COMPLETE'):
            return

        time.sleep(SLEEP_TIME)


def update_cfn_stack():
    change_set_id = create_cfn_change_set()
    if change_set_id is not None:
        execute_cfn_change_set(change_set_id)


def delete_cfn_stack():
    info = get_cfn_stack_info()

    if info is None:
        status = 'NOT_EXIST'
    else:
        status = info['StackStatus']

    if status == 'DELETE_IN_PROGRESS':
        print('CloudFormation stack ' + CFN_STACK_NAME + ' is already being deleted.')
        return
    elif status == 'DELETE_FAILED':
        print(
            'CloudFormation stack ' + CFN_STACK_NAME + ' deletion failed. Please investigate through the AWS console.'
        )
        return
    elif status in ['NOT_EXIST', 'CREATE_FAILED', 'DELETE_COMPLETE']:
        print('CloudFormation stack ' + CFN_STACK_NAME + ' does not exist (status: ' + status + '). Nothing to delete.')
        return
    else:
        print('Warning: This will delete the ' + CFN_STACK_NAME + ' CloudFormation stack and all its resources.')
        response = raw_input('To continue, please type "delete": ')
        if response == 'delete':
            print('Deleting CloudFormation stack: ' + CFN_STACK_NAME)
            cfn = boto3.client('cloudformation', region_name=AWS_DEFAULT_REGION)
            cfn.delete_stack(StackName=CFN_STACK_NAME)
        else:
            print('Aborting...')
            return


def get_state_machine_names():
    stack_output_list = get_cfn_stack_info()['Outputs']
    stack_output_dict = {}
    for output in stack_output_list:
        stack_output_dict[output['OutputKey']] = output['OutputValue']

    result = []
    for state_machine_name in state_machines.keys():
        result.append(stack_output_dict[to_camel_case(state_machine_name) + 'Name'])

    return result


def print_state_machine_names():
    names = get_state_machine_names()
    if len(names) == 0:
        return
    elif len(names) == 1:
        print('State machine name: ' + names[0])
    else:
        print('State machine names:')
        for name in get_state_machine_names():
            print '    ' + name


# Main

@task(default=True)
def deploy():
    check_bucket(LAMBDA_FUNCTION_DEPLOYMENT_BUCKET)
    populate_lambda_functions_dict()
    populate_state_machines_dict()
    update_lambda_function_packages()
    update_cfn_stack()
    print_state_machine_names()


@task()
def delete():
    delete_cfn_stack()
