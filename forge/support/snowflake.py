import json
import logging
from dataclasses import dataclass, asdict, field, is_dataclass
from typing import Text, Dict

import snowflake.connector as snowflake
from snowflake.connector import DictCursor

from forge import DatabaseAdapter, TableMeta
from forge.meta import FieldMeta
from forge.util import col_type

"""
user: Text = 'adam.haskell@pearson.com'
    password: Text = 'Bk3gFn7VctavSeJzV7YOAoPsd'

"""

logger = logging.getLogger("persist.snowflake")


@dataclass
class SnowflakeConnectionInfo(object):
    user: Text
    password: Text
    role: Text
    warehouse: Text
    account: Text
    database: Text
    schema: Text = field(default=None)


@dataclass
class SnowflakeIntegration(DatabaseAdapter):
    connection_info: SnowflakeConnectionInfo

    _connection: 'SnowflakeConnection' = field(init=False, repr=False, default=None)

    @property
    def connection(self):
        if self._connection is None:
            self.connect()
        return self._connection

    def connect(self):
        if self._connection is None:
            logger.info("Connecting to Snowflake...")
            snowflake.paramstyle = 'qmark'
            self._connection = snowflake.connect(**asdict(self.connection_info))
        if self._connection.is_closed():
            self._connection = None
            return self.connect()
        return True

    def close(self):
        self._connection.close()

    def get_count(self):
        self._cursor.count()

    def cursor_for_query(self, query: 'SourceQuery'):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query.qmark, list(query.parameters.values()))
        except  AttributeError:
            cursor.execute(query.qmark, list(query.parameters))
        return cursor

    def discover_schema(self, tbl):
        headers = {}
        cur: 'SnowflakeCursor'
        with self.connection.cursor(DictCursor) as cur:
            cur.execute(f"SHOW terse tables like '{tbl.upper()}'")
            if cur.rowcount:
                cur.execute("SHOW columns IN " + tbl.upper())
                for column_def in cur.fetchall():
                    # required = all([column[5], column[6] is None, column[5] != 'auto_increment'])
                    headers[column_def['column_name'].lower()] = FieldMeta(column_def["column_name"].lower(),
                                                                           json.loads(column_def["data_type"])["type"],
                                                                           [])
        return headers

    def update_schema(self, tbl_meta: TableMeta, diff: Dict[Text, FieldMeta]):
        cur: 'SnowflakeCursor'
        with self.connection.cursor() as cur:
            cur.execute(f"SHOW terse tables like '{tbl_meta.table.upper()}'")
            if not cur.rowcount:
                cur.execute(f"create table {tbl_meta.table.upper()} (new_tbl int )")
            for col_name, col_meta in diff.items():
                if is_dataclass(col_meta.dbtype):
                    continue
                cmd = f"alter table {tbl_meta.table.upper()} " \
                      f"add COLUMN {col_name.upper()} {col_type(col_meta.dbtype)};"
                print(cmd)
                cur.execute(cmd)
        return True
