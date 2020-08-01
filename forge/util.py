import logging
import re
from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal
from typing import Text, Mapping, get_args, get_origin, List

logger = logging.getLogger("persist.util")
try:
    from pyodbc import Error as SqlError
except ImportError:
    SqlError = Exception


def sql_escape(string):
    if string:
        return "'" + string.replace("'", "\\'") + "'"
    else:
        return "null"


def db_safe_name(name):
    import inflect

    s1 = re.sub('([A-Z])', r'_\1', name)[1:]
    s1 = s1.split('_')
    s1 = list(map(lambda x: x.lower(), s1))
    s1[-1] = inflect.engine().plural_noun(s1[-1])
    return '_'.join(s1)


def converter_for(col_type):
    type_only = col_type.split("(")[0]
    if type_only in ("varchar", "nvarchar", "char", "nchar"):
        return lambda x: sql_escape(str(x))
    if type_only in ('float',):
        return lambda x: Decimal(x)
    if type_only in ('datetime', 'timestamp', 'date'):
        return lambda x: x
    if type_only in ('int', 'tinyint'):
        return lambda x: x
    logger.warning("Type unknown {}".format(col_type))
    return lambda x: x


def col_type(python_type):
    # dealing with a generic....
    if get_args(python_type):
        return col_type(get_origin(python_type))
    if issubclass(python_type, (list,dict) ):
        return 'varchar'
    if issubclass(python_type, bool):
        return "boolean"
    if issubclass(python_type, (datetime, date)):
        return 'datetime'
    if issubclass(python_type, str):
        return 'varchar'
    return str(python_type)

class NotSupported(Exception):
    pass


@dataclass
class SourceQuery(object):
    source_query: Text
    parameters: Mapping[Text, Text]
    CURLY_PATTERN = re.compile('\{[^\}]*\}')

    def __str__(self):
        try:
            return self.source_query.format(**self.parameters)
        except TypeError:
            return self.source_query.format(*self.parameters)

    def __repr__(self):
        return self.__str__()

    @property
    def qmark(self):
        return self.convert_markers("?")

    @property
    def named_colon(self):
        return self.convert_markers(lambda a: a[0].replace("{", ":").replace("}"))

    def convert_markers(self, conversion):
        return SourceQuery.CURLY_PATTERN.sub(conversion, self.source_query)

    def x_execute(self, cursor):
        logger.info(self.source_query.replace("?", "'{}'").format(*self.parameters))
        cursor.execute(self.source_query, self.parameters)
        return cursor


"""
class Queryable(IntegrationInterfacing):

    @property
    def queries(self) -> List[SourceQuery]:
        return [self.make_queries()]

    @abstractmethod
    def make_queries(self):
        raise NotImplementedError("")


class SingleQueryable(IntegrationInterfacing):

    @property
    def queries(self):
        return self.make_queries()

    def make_queries(self):
        return [self.make_query()]

    @abstractmethod
    def make_query(self):
        raise NotImplementedError("")


@dataclass
class QueryBasedIntegration(SystemIntegration, ABC):
    _current_query: SourceQuery = field(init=False, repr=False)

    def source(self, source: Queryable = Undefined) -> Queryable:
        return super().source(source)

    def get_queries(self) -> List[SourceQuery]:
        return self.source().queries

    def extract(self):
        for query in self.get_queries():
            yield from self.yield_dataset(self.execute(query))

    def execute(self, query: SourceQuery):
        self._current_query = query
        logger.debug(query)
        for i in range(1, 6):
            try:
                return self.run_query(query)
            except (BaseException, Exception) as ex:
                logger.warning("Query execution failed", exc_info=ex)
                if self.recoverable(ex):
                    time.sleep(5)
                    continue
                else:
                    raise ex
        raise

    def yield_dataset(self, cursor):
        for item in cursor.fetchall():
            self._current_item = item
            yield item

    def run_query(self, query: SourceQuery):
        # query.execute(cursor=self.cursor)
        raise NotImplementedError("Need to execute")

    def recoverable(self, ex):
        return False


@dataclass
class DirectSqlIntegration(QueryBasedIntegration):
    _cursor: Any = field(init=False, default=None)

    def extract_headers(self):
        if "_headers" not in self.__dict__:
            headers = {}
            idx = 0

            for i in self._cursor.description:
                headers[i[0].lower()] = idx
                idx += 1
            if len(headers) != len(self._cursor.description):
                cols = []
                for i in self._cursor.description:
                    if i[0] in cols:
                        msg = f"{i[0]} is in the Select 2 times, snowflake may not care but mapping does."
                        raise RuntimeError(msg)
                    else:
                        cols.append(i[0])
                raise RuntimeError("headers extracted are not the same length as cursor description, something fishy..")

            self._headers = headers
        return self._headers

    def source_info(self):
        from ein.runtime.context import job_context
        if job_context.environment.debug_sql:
            return self._current_query
        else:
            return "direct sql query"

    def run_query(self, query):
        self._cursor = self.cursor_for_query(query)
        return self._cursor

    def cursor_for_query(self, query: SourceQuery):
        raise NotImplementedError("do this not that")
"""
