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
from testflows._core.cli.arg.type import key_value as key_value_type

def argparser(parser):
    parser.add_argument("--database", dest="_database", metavar="name=value", nargs="+",
                        help="""database output handler options, default handler: 'testflows.database.clickhouse'. 
            Options are specific to each output handler. The only required option is 'name'. 
            For the default ClickHouse handler the following extra options can be specified:
                'host=<hostname>'
                'database=<database>'
                'user=<user>'
                'password=<password>'.
            For example: '--database name=local host=localhost'
            """, type=key_value_type, required=False)
