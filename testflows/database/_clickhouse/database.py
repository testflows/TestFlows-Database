# Copyright 2020 Katteli Inc.
# TestFlows.com Open-Source Software Testing Framework (http://testflows.com)
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
import datetime

from testflows.database.base import *


class ColumnTypes(ColumnTypes):
    def __getitem__(self, name):
        if name.startswith("Array"):
            type_name = name.split("(")[-1].strip("()")
            return self.Array(name, self[type_name].convert)
        elif "Int" in name:
            return self.Int(name)
        elif "Float" in name:
            return self.Float(name)
        elif name.startswith("Enum"):
            return self.Enum(name)
        elif name == "String":
            return self.String(name)
        elif name.startswith("DateTime64"):
            precision = name.split("(")[-1].split(",", 1)[0].strip("()")
            return self.DateTime64(name, precision)
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

    @classmethod
    def Float(cls, name):
        def convert(f):
            return repr(f)[1:-1]

        return ColumnType(name, convert, "0")

    @classmethod
    def String(cls, name):
        def convert(s):
            if not s:
                return "''"
            return f"'{json.dumps(s)[1:-1]}'"

        return ColumnType(name, convert, "''")

    @classmethod
    def Enum(cls, name):
        def convert(e):
            if type(e) is int:
                return str(e)
            if not e:
                return "''"
            return f"'{json.dumps(e)[1:-1]}'"

        return ColumnType(name, convert, "''")

    @classmethod
    def Array(cls, name, type_convert):
        def convert(l):
            return f"[{','.join([type_convert(e) for e in l])}]"

        return ColumnType(name, convert, "[]")

    @classmethod
    def Date(cls):
        def convert(d):
            return f"'{d.strftime('%Y-%m-%d')}'"

        return ColumnType('Date', convert, '0')

    @classmethod
    def DateTime(cls):
        def convert(d):
            return f"'{d.strftime('%Y-%m-%d %H:%M:%S')}'"

        return ColumnType('DateTime', convert, '0')

    @classmethod
    def DateTime64(cls, name, precision):
        def convert(d):
            return f"%.{precision}f" % d.timestamp()

        return ColumnType(name, convert, '0')


class Database(Database):
    column_types = ColumnTypes()

    def table(self, name):
        query = f"SELECT name, type FROM system.columns WHERE table = '{name}' AND database = '{self.connection.database}' and default_kind != 'MATERIALIZED'"
        r = self.query(query).any()

        if not r:
            raise KeyError("no table")
        columns = Columns(
            [(entry["name"], Column(entry["name"], idx, self.column_types[entry["type"]])) for idx, entry in
             enumerate(r)])
        return Table(name, self.connection.database, columns)


class DatabaseConnection(DatabaseConnection):
    def __init__(self, host, database, user=None, password=None, port=8123):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.session = requests.Session()
        self.init()
        super(DatabaseConnection, self).__init__()

    def init(self):
        self.url = f"http://{self.host}:{self.port}/"
        self.name = f"{self.database}@{self.url}"
        self.default_params = {
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "input_format_null_as_default": 1
        }

    def reset(self):
        self.close()
        self.init()
        self.open()

    def open(self):
        pass

    def close(self):
        self.session.close()

    def io(self, query, data=False, stream=False, params=None):
        if data is None:
            if query.startswith(("INSERT", "CREATE", "DROP",
                    "SYSTEM", "ALTER", "GRANT", "REVOKE", "ATTACH",
                    "DETACH", "KILL", "SET", "USE", "OPTIMIZE",
                    "RENAME", "TRUNCATE", "PROFILE")):
                data = False
            else:
                data = True

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
        return DatabaseQueryResponse(r, convert=lambda r: json.loads(f"[{','.join(r.text.splitlines())}]"))
