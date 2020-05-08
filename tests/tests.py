#!/usr/bin/env python3
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
from testflows.core import *
from testflows.asserts import error, raises

def query(query, **kwargs):
    self = current()
    with By("executing query", description=f"{query} with {kwargs} options"):
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

@TestFeature
def database_object(self):
    with Given("I import objects"):
        from testflows.database.clickhouse import Database, DatabaseConnection

        with And("I create database object"):
            conn = DatabaseConnection(host="localhost", database="system")
            self.context.database = Database("local", conn)

    query_tests(self)

    with Scenario("work with table"):
        with When("I get table"):
            table = self.context.database.table("one")
        with And("I get a row"):
            row = table.default_row()
        with Then("I check the row"):
            assert str(row) == "Row([('dummy', '0')])", error()
        with And("I check the values"):
            assert list(row.values()) == ["0"], error()
        with When("I set column value"):
            row['dummy'] = 245
        with Then("new column value should be set"):
            assert list(row.values()) == ["245"], error()

@TestFeature
def database_connection_object(self):
    with Given("I import objects"):
        from testflows.database.clickhouse import DatabaseConnection

        with And("I create database connection object"):
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
        run(test=database_connection_object)
        run(test=database_object)

