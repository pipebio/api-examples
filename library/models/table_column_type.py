from enum import Enum


class TableColumnType(Enum):
    INTEGER = 'INT64'
    INT64 = 'INT64'

    STRING = 'STRING'
    BYTES = 'BYTES'
    BOOLEAN = 'BOOLEAN'
    NUMERIC = 'NUMERIC'
    ARRAY = 'ARRAY'
    STRUCT = 'STRUCT'
