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
from collections import OrderedDict
from collections.abc import Generator
from collections import namedtuple

__all__ = [
        "Database",
        "DatabaseConnection",
        "DatabaseQueryResponse",
        "DatabaseQueryStreamResponse",
        "DatabaseError",
        "DatabaseConnectionError",
        "DatabaseQueryError",
        "DatabaseQueryNoEntries",
        "DatabaseQueryMultipleEntries",
        "ColumnTypes",
        "ColumnType",
        "Table",
        "Column",
        "Columns"
    ]

class DatabaseError(Exception):
    pass

class DatabaseQueryError(DatabaseError):
    pass

class DatabaseQueryNoEntries(DatabaseQueryError):
    pass

class DatabaseQueryMultipleEntries(DatabaseQueryError):
    pass

class DatabaseConnectionError(DatabaseError):
    pass

class DatabaseConnection:
    def __init__(self):
        pass
    
    def open(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def reset(self):
        self.close()
        self.open()

    def io(self, query, data, stream=False, params=None):
        raise NotImplementedError

class DatabaseQueryStreamResponse(Generator):
    def __init__(self, response, convert):
        self.response = response
        self.convert = convert
        self.iter = iter(self.response.iter_lines())

    def send(self, _):
        try:
            r = next(self.iter)
            return self.convert(r)
        except Exception as e:
            self.response.connection.close()
            raise

    def throw(self, type=None, value=None, traceback=None):
         raise StopIteration

class DatabaseQueryResponse:
    def __init__(self, response, convert):
        self.response = response
        self.data = convert(response)

    def one(self):
        if not self.data:
            raise DatabaseQueryNoEntries
        if len(self.data) > 1:
            raise DatabaseQueryMultipleEntries(json.dumps(self.data[:5], indent=2))
        return self.data[0]

    def first(self):
        if not self.data:
            raise DatabaseQueryNoEntries
        return self.data[0]

    def last(self):
        if not self.data:
            raise DatabaseQueryNoEntries
        return self.data[-1]

    def all(self):
        if not self.data:
            raise DatabaseQueryNoEntries
        return self.data

    def any(self):
        return self.data

class Row(OrderedDict):
    def __init__(self, columns):
        self.columns = columns
        data = [(col.name, col.type.default_value) for col in columns.values()]
        return super(Row, self).__init__(data)

    def __setitem__(self, key, value):
        try:
            col_name, col_index, col_type = self.columns[key]
        except KeyError:
            raise KeyError(f"'{key}' no such column") from None
        value = col_type.convert(value)
        return super().__setitem__(key, value)

Column = namedtuple("Column", "name index type")

class Columns(OrderedDict):
    def __init__(self, columns):
        return super(Columns, self).__init__(columns)

class Table:
    def __init__(self, columns):
        self.columns = columns

    def default_row(self):
        return Row(self.columns)

ColumnType = namedtuple("ColumnType", "name convert default_value")

class ColumnTypes:
    def __getitem__(self, name):
        return getattr(self, name)()

class Database:
    column_types = ColumnTypes()

    def __init__(self, name, connection):
        self.name = name
        self.connection = connection

    def table(self, name):
        raise NotImplementedError

    def query(self, query, data=True, stream=False, params=None):
        return self.connection.io(query, data=data, stream=stream, params=params)
