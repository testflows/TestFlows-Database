# Copyright 2020 Katteli Inc., TestFlows Test Framework (http://testflows.com)
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
import json
import requests

from testflows.database.base import *

class ColumnTypes(ColumnTypes):
    def __getitem__(self, name):
        if "Int" in name:
            return self.Int(name)
        return getattr(self, name)()

    @classmethod
    def Int(cls, name):
        def convert(i):
            if i == 0:
                return '0'
            elif i == 1:
                return '1'
            return str(i)
        return ColumnType(name, convert, "0")

class Database(Database):
    column_types = ColumnTypes()

    def table(self, name):
        query = f"SELECT name, type FROM system.columns WHERE table = '{name}' AND database = '{self.connection.database}'"
        r = self.query(query).any()

        if not r:
            raise KeyError("no table")
        columns = Columns([(entry["name"], Column(entry["name"], idx, self.column_types[entry["type"]])) for idx, entry in enumerate(r)])
        return Table(columns)

class DatabaseConnection(DatabaseConnection):
    def __init__(self, host, database, user=None, password=None, port=8123):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.session = requests.Session()
        self.url = f"http://{self.host}:{self.port}/"
        self.default_params = {
            "user": self.user,
            "password": self.password,
            "database": self.database
        }
        super(DatabaseConnection, self).__init__()

    def open(self):
        pass

    def close(self):
        self.session.close()

    def io(self, query, data=False, stream=False, params=None):
        if data:
            query += " FORMAT JSONEachRow"

        params = dict(params or {})
        params.update(self.default_params)

        try:
            r = self.session.post(self.url, data=query, params=params, stream=stream)
        except Exception as exc:
            raise DatabaseConnectionError from exc
        try:
            r.raise_for_status()
        except Exception as exc:
            raise DatabaseError(r.text) from exc

        if not data:
            return r

        if stream:
            return DatabaseQueryStreamResponse(r, convert=json.loads)
        return DatabaseQueryResponse(r, convert = lambda r : json.loads(f"[{','.join(r.text.splitlines())}]"))
