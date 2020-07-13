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
    name = None

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

class Row:
    def __init__(self, columns, default_data=None):
        self.columns = columns
        if default_data is None:
            default_data = self.default_data(columns)
        self.data = OrderedDict(default_data)

    @classmethod
    def default_data(cls, columns):
        return [(col.name, col.type.default_value) for col in columns.values()]

    def __str__(self):
        return f'Row{str(self.data).lstrip("OrderedDict")}'

    def __repr__(self):
        return f'Row{repr(self.data).lstrip("OrderedDict")}'

    def values(self):
        return self.data.values()

    def keys(self):
        return self.data.keys()

    def __getitem__(self, index):
        return self.data[index]

    def __setitem__(self, key, value, raw=False):
        try:
            col_name, col_index, col_type = self.columns[key]
        except KeyError:
            raise KeyError(f"'{key}' no such column") from None
        if not raw:
            value = col_type.convert(value)
        self.data[key] = value

Column = namedtuple("Column", "name index type")

class Columns(OrderedDict):
    def __init__(self, columns):
        return super(Columns, self).__init__(columns)

class Table:
    def __init__(self, name, database, columns):
        self.name = name
        self.database = database
        self.columns = columns
        self.row_default_data = Row.default_data(columns)

    def default_row(self):
        return Row(self.columns, default_data=self.row_default_data)

ColumnType = namedtuple("ColumnType", "name convert default_value")

class ColumnTypes:
    def __getitem__(self, name):
        return getattr(self, name)()

class Database:
    column_types = ColumnTypes()

    def __init__(self, connection):
        self.connection = connection

    @property
    def name(self):
        return self.connection.name

    def table(self, name):
        raise NotImplementedError

    def query(self, query, data=None, stream=False, params=None):
        return self.connection.io(query, data=data, stream=stream, params=params)
