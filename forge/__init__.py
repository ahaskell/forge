import datetime
import logging
from dataclasses import dataclass, field, is_dataclass, fields
from typing import List

from forge.meta import FieldMeta, TableMeta
from forge.support import DatabaseAdapter
from forge.support.snowflake import SnowflakeConnectionInfo

Datatime = datetime.datetime


def discover_integration():
    import persist_config
    print(persist_config)


@dataclass
class Forge(object):
    models: List['dataclass'] = field(default_factory=list)
    _integration: DatabaseAdapter = field(default=None)

    @property
    def integration(self) -> DatabaseAdapter:
        if self._integration is None:
            discover_integration()
        return self._integration

    def register(self, table_meta: TableMeta):
        cls_schema = self.build_schema(table_meta.datacls)
        db_schema = self.integration.discover_schema(table_meta.table)
        diff = self.schema_diff(cls_schema, db_schema)
        if len(diff):
            logging.getLogger("damp").info("Database schema not up to date")
            self.integration.update_schema(table_meta, diff)
        self.models.append(table_meta)

    def register_integration(self, integration):
        self._integration = integration

    def build_schema(self, dtcls):
        schema = {}
        for dt_field in fields(dtcls):
            schema[dt_field.name] = self.column_meta(dt_field)
        return schema

    def column_meta(self, dt_field):
        if isinstance(dt_field.metadata, FieldMeta):
            meta = dt_field.metadata
        else:
            meta = dt_field.metadata.get("persist", FieldMeta(dt_field.name, dt_field.type))
        return meta

    def schema_diff(self, cls_schema, db_schema):
        diff = {}
        db_columns = db_schema.keys()
        for col in cls_schema.keys():
            if col not in db_columns:
                diff[col] = cls_schema[col]
        return diff


forge = Forge()


def persist(cls=None, /, *, meta=None, schema=None, table=None):
    def wrapper(cls):
        return _process_class(cls, meta, schema, table)

    if cls is None:
        # parameters sent return the wrapper to decorate
        return wrapper

    # called with nothing as @persist we'll do everthing, return a processed class
    return wrapper(cls)


def _process_class(datacls, tbl_meta, schema, table):
    if not is_dataclass(datacls):
        logging.getLogger("damp").warning(
            f"{datacls} wrapped with persist but is not a data class, making it a data class first"
        )
        datacls = dataclass(datacls)
    if tbl_meta is None:
        tbl_meta = TableMeta(datacls, schema, table)
    forge.register(tbl_meta)
    if "save" not in datacls.__dict__:
        datacls.save = lambda s, *a, **kw: tbl_meta.save(s)
    return datacls
