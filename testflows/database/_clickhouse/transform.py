# Copyright 2019 Vitaliy Zakaznikov (TestFlows Test Framework http://testflows.com)
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
import concurrent.futures

from testflows._core.transform.log import message

def format_version(self, msg, row):
    pass

formatters = {
    message.RawVersion: (format_version),
}

def write(database, query):
    database.query(query)

def flush(self):
    with self.buffer as buffer:
        if not buffer:
            return
        query = f"{self.query} {','.join([''.join(['(', ','.join(row.values()), ')']) for row in buffer])}"
        del buffer[:]
    self.tasks.append(self.workers.submit(write, self.database, query))

class State:
    pass

class Buffer():
    def __init__(self):
        self.buffer = []
        self.lock = threading.Lock()
        super(Buffer, self).__init__()

    def __enter__(self):
        self.lock.acquire()
        return self.buffer

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lock.release()


def transform(database, table="messages"):
    """Write to ClickHouse database.

    :param database: database object
    :param table: table name, default: 'messages'
    """
    self = State()
    self.buffer = Buffer()
    self.database = database
    self.table = self.database.table(table)
    self.query = f"INSERT INTO {self.table.name} ({','.join([col for col in self.table.columns])}) VALUES"
    self.tasks = []

    line = None
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as workers:
        self.workers = workers

        while True:
            if line is not None:
                formatter = formatters.get(type(line), None)
                #if not formatter:
                #    continue
                row = self.table.default_row()

                #formatter(line, row)
                row["message_num"] = line.p_num
                row["message_keyword"] = line.p_keyword
                row["message_hash"] = line.p_hash
                row["message_stream"] = line.p_stream
                # FIXME: p_time does not make senses
                # p_time should be message_time, test_time should be relative
                #message_time = repr(line.p_time)[1:-1]
                #row.__setitem__("message_time",  message_time, raw=True)
                #row.__setitem__("message_date", f"toDate({message_time})", raw=True)
                row["test_type"] = line.p_type
                row["test_subtype"] = line.p_subtype
                row["test_id"] = line.p_id
                row["test_flags"] = line.p_flags
                row["test_cflags"] = line.p_cflags

                with self.buffer as buffer:
                    buffer.append(row)

                flush(self)
                for task in concurrent.futures.as_completed(self.tasks):
                    task.result()

            line = yield line