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
import concurrent.futures

auto_flush_interval = 0.25

def write(database, query):
    database.query(query)

def flush(self, final=False):
    self.timer.cancel()
    query = None

    with self.buffer as buffer:
        if buffer:
            query = f"{self.query} {' '.join(buffer)}"
            del buffer[:]

    if query:
        self.tasks.append(self.workers.submit(write, self.database, query))

    if not final:
        done, pending = concurrent.futures.wait(self.tasks, return_when=concurrent.futures.FIRST_COMPLETED)
        for task in done:
            task.result()
        self.tasks = list(pending)
    else:
        for task in self.tasks:
            task.result()

    if not final:
        self.timer = set_timer(self)

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

def set_timer(self):
    timer = threading.Timer(auto_flush_interval, flush, (self,))
    timer.start()
    return timer

def transform(database, stop, table="messages"):
    """Write to ClickHouse database.

    :param database: database object
    :param stop: stop event
    :param table: table name, default: 'messages'
    """
    self = State()
    self.buffer = Buffer()
    self.database = database
    self.table = self.database.table(table)
    self.query = f"INSERT INTO {self.table.name} FORMAT JSONEachRow "
    self.tasks = []
    self.timer = set_timer(self)

    msg = None
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as workers:
        self.workers = workers

        while True:
            if msg is not None:
                with self.buffer as buffer:
                    buffer.append(msg)

                if stop is not None and stop.is_set():
                    flush(self, final=True)

            msg = yield msg
