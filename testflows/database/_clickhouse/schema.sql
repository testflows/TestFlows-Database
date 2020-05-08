-- Copyright 2020 Vitaliy Zakaznikov (TestFlows Test Framework http://testflows.com)
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
DROP TABLE IF EXISTS messages;
CREATE TABLE messages
(
    message_keyword Enum8(
        'NONE' = 0,
        'TEST' = 1,
        'NULL' = 2,
        'OK' = 3,
        'FAIL' = 4,
        'SKIP' = 5,
        'ERROR' = 6,
        'EXCEPTION' = 7,
        'VALUE' = 8,
        'NOTE' = 9,
        'DEBUG' = 10,
        'TRACE' = 11,
        'XOK' = 12,
        'XFAIL' = 13,
        'XERROR' = 14,
        'XNULL' = 15,
        'PROTOCOL' = 16,
        'INPUT' = 17,
        'VERSION' = 18,
        'METRIC' = 19,
        'TICKET' = 20
    ),
    message_hash String,
    message_num UInt64,
    message_stream String,
    message_time DateTime64(6),
    message_date Date,
    test_type Enum8 (
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
        'Empty' = 0
    ),
    test_id String,
    test_flags UInt64,
    test_cflags UInt64,
    test_name String,
    test_started DateTime64(6),
    test_flags UInt64,
    test_uid String,
    test_description String,
    test_attributes Nested(
        name String,
        value String,
        type String,
        group String,
        uid String
    ),
    test_requirements Nested(
        name String,
        version String,
        description String,
        link String,
        priority String,
        type String,
        group String,
        uid String
    ),
    test_args Nested(
        name String,
        value String,
        group String,
        type String,
        uid String
    ),
    test_tags Nested(
        value String
    ),
    test_users Nested(
        name String,
        type String,
        group String,
        link String,
        uid String
    ),
    test_users_tags Nested(
        object_id String,
        value String
    ),
    test_users_attributes Nested(
        object_id String,
        name String,
        value String,
        type String,
        group String,
        uid String
    ),
    -- test_examples Nested(),
    -- test_node
    -- test_map
    protocol_version String,
    framework_version String,
    input_message String,
    exception_message String,
    note_message String,
    debug_message String,
    trace_message String,
    user_name String,
    user_type String,
    user_group String,
    user_link String,
    user_uid String,
    ticket_name String,
    ticket_link String,
    ticket_type String,
    ticket_group String,
    ticket_uid String,
    value_name String,
    value_value String,
    value_type String,
    value_group String,
    value_uid String,
    metric_name String,
    metric_value String,
    metric_units String,
    metric_type String,
    metric_group String,
    metric_uid String,
    result_xout UInt8,
    result_ok UInt8,
    result_xok UInt8,
    result_fail UInt8,
    result_xfail UInt8,
    result_error UInt8,
    result_xerror UInt8,
    result_null UInt8,
    result_xnull UInt8,
    result_skip UInt8,
    result_name String,
    result_test String,
    result_message String,
    result_reason String,
    result_metrics Nested (
        name String,
        value String,
        units String,
        type String,
        group String,
        uid String
    ),
    result_tickets Nested (
        name String,
        link String,
        type String,
        group String,
        uid String
    ),
    result_values Nested (
        name String,
        value String,
        type String,
        group String,
        uid String
    )
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(message_date)
ORDER BY (test_id, message_num);
