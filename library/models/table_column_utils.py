from typing import Union, Type

from numpy import float64, array, int64, long
from pandas import BooleanDtype

from library.models.table_column_type import TableColumnType


def table_column_type_to_numpy_type(column_type: TableColumnType) -> Union[str, Type[array], Type[long]]:
    if column_type == TableColumnType.INT64:
        return int64
    elif column_type == TableColumnType.INTEGER:
        return int64
    elif column_type == TableColumnType.STRING:
        return str
    elif column_type == TableColumnType.NUMERIC:
        return float64
    elif column_type == TableColumnType.BOOLEAN:
        return BooleanDtype
    elif column_type == TableColumnType.ARRAY:
        return array
    else:
        raise 'No conversion specified for datatype "{}" to Numpy/Dask'.format(column_type.value)
