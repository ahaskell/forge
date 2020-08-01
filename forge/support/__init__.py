from typing import Dict, Text, get_args, get_origin, List

from forge import FieldMeta, TableMeta


class DatabaseAdapter:
    def discover_schema(self, tbl):
        raise NotImplementedError()

    def update_schema(self, tbl_meta:TableMeta, diff:Dict[Text, FieldMeta]):
        raise NotImplementedError()


class LexiconConvention(object):
    """ This Class attempts to easy adapter development by providing a declaritive approach to the most common
    tasks for converting Python classes and data to persistable things.
    """

