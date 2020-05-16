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
import threading

from testflows._core.transform.log.pipeline import Pipeline
from testflows._core.transform.log.read import transform as read_transform
from testflows._core.transform.log.parse import transform as parse_transform
from testflows._core.transform.log.stop import transform as stop_transform
from testflows.database.clickhouse import transform as write_to_database_transform

class WriteToDatabasePipeline(Pipeline):
    def __init__(self, input, database, tail=False):
        stop_event = threading.Event()

        steps = [
            read_transform(input, tail=tail),
            parse_transform(stop_event),
            write_to_database_transform(database),
            stop_transform(stop_event)
        ]
        super(WriteToDatabasePipeline, self).__init__(steps)
