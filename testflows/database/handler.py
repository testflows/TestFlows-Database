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
import testflows.settings as settings

from testflows.database.pipeline import WriteToDatabasePipeline
from testflows.database.clickhouse import Database, DatabaseConnection

def database_handler():
    """Handler to write output messages to database.
    """
    options = {option.key: option.value for option in settings.database}

    conn = DatabaseConnection(
        host=options.pop("host", "localhost"),
        database=options.pop("database", "default"),
        user=options.pop("user", None),
        password=options.pop("password", None),
        port=options.pop("port", 8123)
    )

    database = Database(name=options.pop("name"), connection=conn)

    with open(settings.read_logfile, "a+", buffering=1, encoding="utf-8") as log:
        log.seek(0)
        WriteToDatabasePipeline(log, database, tail=True).run()
