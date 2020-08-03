import datetime
from collections import UserDict
from dataclasses import dataclass, field
from typing import Text, List, Type, Dict

from forge.util import db_safe_name

Datatime = datetime.datetime


@dataclass
class FieldMeta(UserDict):
    """Most of the time Field Meta can be generated by DAMP but in some cases meta is needed to help configure
    DAMP. Use case include mapping a field to a column by a different name, or defining an index Field Meta inherits
    from MappingProxyType so it cancbe set directly as the metadata on a field:
     `field(metadata=FieldMeta(column='jr')) however DAMP will also look for a persist key if the metadata object
     not a FieldMeta object. """

    column: Text
    """The column this field maps to in the target database table schema"""
    dbtype: Type
    """A database Mappable Python type (generally should be all types but IDK)"""
    indexes: List[Text] = field(default_factory=list)
    """A list of index names this field should be included in. instead of defining an index as 
    `index_1 = ['name','email','year']` fields are "added" into indexes by naming the indexes here. The default 
    order in the index is ASC, if the order should be DESC add a - to the index name -my_speedy_idx"""

@dataclass
class TableMeta(object):
    datacls: 'dataclass' = field()
    schema: Text = field(default=None)
    table: Text = field(default=None)
    indexes: Dict[Text, List[Text]] = field(default_factory=dict)

    def __post_init__(self):
        if self.table is None:
            self.table = db_safe_name(self.datacls.__name__)

    def save(self, dataobj):
        print("saving", dataobj)