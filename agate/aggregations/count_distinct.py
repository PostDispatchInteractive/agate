#!/usr/bin/env python

from agate.aggregations.base import Aggregation
from agate.data_types import Number
from agate.utils import default


class CountDistinct(Aggregation):
    """
    Count occurences of a value or values.

    This aggregation can be used in two ways:

    1. If no arguments are specified, then it will count the number of rows in the table.
    2. If only :code:`column_name` is specified, then it will count the number of distinct values in a column.

    :param column_name:
        The column containing the values to be counted.
    """
    def __init__(self, column_name=None):
        self._column_name = column_name

    def get_aggregate_data_type(self, table):
        return Number()

    def run(self, table):
        if self._column_name is not None:
            return len(table.columns[self._column_name].values_distinct())
        else:
            return len(table.rows)
