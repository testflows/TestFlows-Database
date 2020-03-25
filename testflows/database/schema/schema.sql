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
    m_keyword Enum8(
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
    m_hash String,
    m_num UInt64,
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
    m_stream String,
    m_time DateTime64(6),
    m_date Date
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(m_date)
ORDER BY (test_id, m_num);