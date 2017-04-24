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
# Description: "Combine a list of dicts into a single output dict. Useful for AWS Step Functions parallel tasks."
# MemorySize: 128
# Timeout: 10
# Policies:
# ---
#
# Input event: A list of dicts.
# Output: A single dict with all attributes of the input dicts merged in.
#


def handler(event, _):
    assert(isinstance(event, list))

    result = {}
    for i in event:
        result.update(i)

    return result
