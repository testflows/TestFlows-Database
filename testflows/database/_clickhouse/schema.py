#!/usr/bin/env python3
# Copyright 2020 Katteli Inc.
# TestFlows Test Framework (http://testflows.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
schema = [
"DROP TABLE IF EXISTS messages",
"""CREATE TABLE messages
(
    message_keyword Enum8(
        'NONE' = 0,
        'TEST' = 1,
        'RESULT' = 2,
        'EXCEPTION' = 3,
        'NOTE' = 4,
        'DEBUG' = 5,
        'TRACE' = 6,
        'VERSION' = 7,
        'PROTOCOL' = 8,
        'INPUT' = 9,
        'VALUE' = 10,
        'METRIC' = 11,
        'TICKET' = 12,
        'ARGUMENT' = 13,
        'TAG' = 14,
        'ATTRIBUTE' = 15,
        'REQUIREMENT' = 16,
        'EXAMPLE' = 17,
        'NODE' = 18,
        'MAP' = 19,
        'STOP' = 20
    ),
    message_hash String,
    message_object UInt16,
    message_test_object UInt8 MATERIALIZED bitAnd(message_object, 0x1) > 0,
    message_num UInt32,
    message_stream String,
    message_level UInt32,
    message_time Float64,   
    message_rtime Float64,
    message_date Date MATERIALIZED toDate(message_time),
    -- test
    test_type Enum8 (
        '' = 0,
        'Module' = 40,
        'Suite' = 30,
        'Test' = 20,
        'Iteration' = 15,
        'Step' = 10
    ),
    test_subtype Enum8 (
        'Feature' = 60,
        'Scenario' = 50,
        'Background' = 40,
        'Given' = 30,
        'When' = 20,
        'Then' = 10,
        'And' = 8,
        'But' = 7,
        'By' = 6,
        'Finally' = 5,
        '' = 0
    ),
    test_id String,
    test_key FixedString(16) MATERIALIZED sipHash128(test_id),
    test_parent String MATERIALIZED extract(test_id, '(.+)/.+'),
    test_top String MATERIALIZED extract(test_id, '(/[^/]+)'),
    test_istop UInt8 MATERIALIZED test_parent = '',
    test_flags UInt32,
    test_cflags UInt32,
    test_level UInt32,
    -- parent ids
    project_id String,
    user_id String,
    environment_id String,
    device_id String,
    software_id String,
    hardware_id String,
    product_id String,
    map_id String,
    -- test
    test_name String,
    test_uid String,
    test_description String,
    -- attribute
    attribute_name String,
    attribute_value String,
    attribute_type String,
    attribute_group String, 
    attribute_uid String,
    -- requirement
    requirement_name String,
    requirement_version String,
    requirement_description String,
    requirement_link String,
    requirement_priority String,
    requirement_type String,
    requirement_group String,
    requirement_uid String,
    -- argument
    argument_name String,
    argument_value String,
    argument_type String,
    argument_group String,
    argument_uid String,
    -- tag
    tag_value String,
    -- user
    user_name String,
    user_type String,
    user_group String,
    user_link String,
    user_uid String,
    -- example
    example_row UInt32,
    example_columns Array(String),
    example_values Array(String),
    example_row_format String,
    -- node
    node_name String,
    node_module String,
    node_uid String,
    node_nexts Array(String),
    node_ins Array(String),
    node_outs Array(String),
    -- simple messages
    protocol_version String,
    framework_version String,
    message String,
    -- ticket    
    ticket_name String,
    ticket_link String,
    ticket_type String,
    ticket_group String,
    ticket_uid String,
    -- value
    value_name String,
    value_value String,
    value_type String,
    value_group String,
    value_uid String,
    -- metric
    metric_name String,
    metric_value Float64,
    metric_units String,
    metric_type String,
    metric_group String,
    metric_uid String,
    -- result
    result_message String,
    result_reason String,
    result_type Enum8(
        '' = 0,
        'OK' = 1,
        'Fail' = 2,
        'Error' = 3,
        'Null' = 4,
        'Skip' = 5,
        'XOK' = 6,
        'XFail' = 7,
        'XError' = 8,
        'XNull' = 9
    ),
    result_test String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(message_date)
ORDER BY (test_id, message_num)"""
]

if __name__ == "__main__":
    print(";\n".join(schema))
