# Copyright 2020 Vitaliy Zakaznikov, TestFlows Test Framework (http://testflows.com)
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







