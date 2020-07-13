#!/usr/bin/env python3
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
import uuid
import time
import datetime

from collections import namedtuple

from testflows.core import *
from testflows.asserts import error, raises

def query(query, **kwargs):
    self = current()
    with By("executing query", description=f"{query.strip()}\nwith {kwargs} options", format_description=False):
        if "database" in self.context:
            return self.context.database.query(query, **kwargs)
        elif "connection" in self.context:
            return self.context.connection.io(query, **kwargs)
        raise ValueError("no proper context")

def run_simple_query():
    r = query("SELECT 1", data=True)

    with Then("it should work"):
        assert r.data == [{"1": 1}], error()

    return r

def query_tests(self):
    with Scenario("simple query"):
        with When("I run simple query"):
            run_simple_query()

    with Scenario("query that returns multiple rows"):
        with When("I run a query"):
            r = query("SELECT number FROM system.numbers LIMIT 10", data=True)

        with Then("check number of entries"):
            assert len(r.any()) == 10, error()

    with Scenario("check one() method"):
        with When("I run a query that return one entry"):
            r = query("SELECT number FROM system.numbers LIMIT 1", data=True)
        with And("I call one() method"):
            v = r.one()
        with Then("it should return the only entry"):
            assert v == {"number":"0"}, error()

        with When("I run a query that returns no entries"):
            r = query("SELECT * FROM system.one WHERE dummy > 1", data=True)
        with Then("call to one() method should raise an exception"):
            from testflows.database.clickhouse import DatabaseQueryNoEntries
            with raises(DatabaseQueryNoEntries):
                r.one()

        with When("I run a query that returns multiple entries"):
            r = query("SELECT number FROM system.numbers LIMIT 10", data=True)
        with Then("call to one() method should raise an exception"):
            from testflows.database.clickhouse import DatabaseQueryMultipleEntries
            with raises(DatabaseQueryMultipleEntries):
                r.one()

    with Scenario("check any() method"):
        with When("I run a query that returns one entry"):
            r = run_simple_query()
        with And("I call the method"):
            v = r.any()
        with Then("it should return all the entries"):
            assert v == [{"1":1}], error()

        with When("I run a query that returns no entries"):
            r = query("SELECT * FROM system.one WHERE dummy > 1", data=True)
        with Then("call to method should not raise an exception"):
            r.any()

    with Scenario("check all() method"):
        with When("I run a query that returns one entry"):
            r = run_simple_query()
        with And("I call the method"):
            v = r.all()
        with Then("it should return all the entries"):
            assert v == [{"1":1}], error()

        with When("I run a query that returns no entries"):
            r = query("SELECT * FROM system.one WHERE dummy > 1", data=True)
        with Then("call to method should raise an exception"):
            from testflows.database.clickhouse import DatabaseQueryNoEntries
            with raises(DatabaseQueryNoEntries):
                r.all()

    with Scenario("check first() method"):
        with When("I run a query that returns multiple entries"):
            r = query("SELECT number FROM system.numbers LIMIT 10", data=True)
        with And("I call the method"):
            v = r.first()
        with Then("it should return the first entry only"):
            assert v == {"number":"0"}, error()

        with When("I run a query that returns no entries"):
            r = query("SELECT * FROM system.one WHERE dummy > 1", data=True)
        with Then("call to method should raise an exception"):
            from testflows.database.clickhouse import DatabaseQueryNoEntries
            with raises(DatabaseQueryNoEntries):
                r.first()

    with Scenario("check last() method"):
        with When("I run a query that returns multiple entries"):
            r = query("SELECT number FROM system.numbers LIMIT 10", data=True)
        with And("I call the method"):
            v = r.last()
        with Then("it should return the last entry only"):
            assert v == {"number":"9"}, error()

        with When("I run a query that returns no entries"):
            r = query("SELECT * FROM system.one WHERE dummy > 1", data=True)
        with Then("call to method should raise an exception"):
            from testflows.database.clickhouse import DatabaseQueryNoEntries
            with raises(DatabaseQueryNoEntries):
                r.last()

    with Scenario("check streaming"):
        with When("I run a query that returns large number of entries with stream mode"):
            r = query("SELECT number FROM system.numbers LIMIT 10000", data=True, stream=True)
        with And("I consume the entries"):
            data = [entry for entry in r]
        with Then("entries should match the expected"):
            assert data[0] == {"number":"0"}, error()
            assert data[-1] == {"number":"9999"}, error()

@TestStep
def create_test_database(self):
    database_name = f'test_{uuid.uuid1()}'.replace("-", "")

    def callback():
        with Finally("I drop the database"):
            if "database" in self.context:
                query(f"DROP DATABASE IF EXISTS {database_name}")

    self.context.cleanup(callback)

    with By("creating database object"):
        from testflows.database.clickhouse import Database, DatabaseConnection
        conn = DatabaseConnection(host="localhost", database="default")
        db = Database(conn)
        self.context.database = db

    with And("creating a test database"):
        query(f"DROP DATABASE IF EXISTS {database_name}")
        query(f"CREATE DATABASE {database_name}")
        db.connection.database = database_name
        db.connection.reset()

@TestFeature
def database_object(self):
    with Given("I import objects"):
        from testflows.database.clickhouse import Database, DatabaseConnection

        with When("I create database object"):
            conn = DatabaseConnection(host="localhost", database="system")
            self.context.database = Database(conn)

    query_tests(self)

    with Scenario("use table"):
        with When("I get table"):
            table = self.context.database.table("one")
            with When("I get a row"):
                row = table.default_row()
            with Then("I check the row"):
                assert str(row) == "Row([('dummy', '0')])", error()
            with And("I check the values"):
                assert list(row.values()) == ["0"], error()

        with When("I set column value"):
            row['dummy'] = 245
            with Then("new column value should be set"):
                assert list(row.values()) == ["245"], error()

    with Scenario("check types") as self:
        table_name = "supported_types"
        Column = namedtuple("Column", "name type")

        columns = [
            Column("int", "Int8"),
            Column("uint", "UInt64"),
            Column("float", "Float64"),
            Column("str", "String"),
            Column("date", "Date"),
            Column("dt", "DateTime"),
            Column("dt64", "DateTime64"),
            Column("a_int", "Array(Int8)"),
            Column("enum", "Enum8(''=10, 'zero'=0, 'one'=1, 'two'=2)"),
            Column("nested", "Nested(str String, int Int8)")
        ]

        with Given("I have a database"):
            with By("creating test database"):
                create_test_database()

            with And("creating a table that uses all the supported types"):
                query(f"DROP TABLE IF EXISTS {table_name}")
                query(f"CREATE TABLE {table_name} (\n"
                    + ",\n".join(["    %s %s" % (col.name, col.type) for col in columns])
                    + "\n) ENGINE = Memory()")

        with When("I get table"):
            table = self.context.database.table(table_name)
        with And("I get a row"):
            row = table.default_row()

        with When("I set columns of types"):
            with By("setting Int8 column"):
                row["int"] = -5
            with And("UInt64 column"):
                row["uint"] = 6
            with And("Float64 column"):
                row["float"] = 12234.22345
            with And("String column"):
                row["str"] = "hello\nthere\t\bvoo"
            with And("Date column"):
                row["date"] = datetime.datetime(2020,1,1)
            with And("DateTime column"):
                row["dt"] = datetime.datetime(2020,1,1)
            with And("DateTime64 column"):
                row["dt64"] = datetime.datetime(2020,1,1)
            with And("Array(Int8) column"):
                row["a_int"] = [8,5,68]
            with And("Enum8 column"):
                row["enum"] = "two"
            with And("Nested column"):
                row["nested.str"] = ["hello"]
                row["nested.int"] = [123]

        with When("I insert the row into the table"):
            values = ",".join(row.values())
            note(values)
            query(f"INSERT INTO {table_name} VALUES ({values})")

            with And("I read the row back"):
                r = query(f"SELECT * FROM {table_name}").one()

            with Then("data should match"):
                expected = {'int': -5, 'uint': '6',
                    'float': 2234.2234, 'str': 'hello\nthere\t\x08voo',
                    'date': '2020-01-01', 'dt': '2020-01-01 00:00:00',
                    'dt64': '2020-01-01 00:00:00.000', 'a_int': [8, 5, 68],
                    'enum': 'two', 'nested.str': ['hello'], 'nested.int': [123]}
                assert r == expected, error()

    with Scenario("use schema") as self:
        with Given("I have a database"):
            with By("creating test database"):
                create_test_database()

            with And("loading schema"):
                from testflows.database.clickhouse import schema
                for statement in schema:
                    query(statement)

        with When("I get messages table"):
            table = self.context.database.table("messages")
        with And("I get a row"):
            row = table.default_row()

        with When("I set column value"):
            row["message_num"] = 100
            with Then("check new value was set"):
                assert row["message_num"] == '100', error()

        with When("I create many rows"):
            start_time = time.time()
            for i in range(100000):
                table.default_row()
            delta = time.time() - start_time

            metric("create_100k_rows_time", value=delta, units="sec")
            with Then("it should not take too long"):
                assert delta < 3, error()

        with When("I make sure rows can store different data"):
            with By("creating two rows"):
                row1 = table.default_row()
                row2 = table.default_row()
            with And("setting the same column to different values"):
                row1["attribute_name"] = ["attr1"]
                row2["attribute_name"] = ["attr2"]
            with Then("values of the column should not change"):
                assert row1["attribute_name"] == '\'"attr1"\'', error()
                assert row2["attribute_name"] == '\'"attr2"\'', error()


@TestFeature
def database_connection_object(self):
    with Given("I import objects"):
        from testflows.database.clickhouse import DatabaseConnection

        with When("I create database connection object"):
            self.context.connection = DatabaseConnection("localhost", "default")

    with Scenario("reset connection"):
        with When("I reset connection"):
            self.context.connection.reset()
        with And("I run simple query"):
            run_simple_query()

    with Scenario("close connection"):
        with When("I close connection"):
            self.context.connection.close()
        with And("I run simple query"):
            run_simple_query()

    with Scenario("open connection"):
        with When("I open connection"):
            self.context.connection.open()
        with And("I run simple query"):
            run_simple_query()

    query_tests(self)

if main():
    with Module("regression"):
        Feature(run=database_connection_object)
        Feature(run=database_object)

