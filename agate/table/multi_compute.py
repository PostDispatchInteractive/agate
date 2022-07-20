#!/usr/bin/env python
# pylint: disable=W0212

from multiprocessing import Pool
from collections import OrderedDict
from copy import copy

from agate.rows import Row


def multi_compute(self, computations, replace=False):
    """
    Create a new table by applying one or more :class:`.Computation` instances
    to each row.

    :param computations:
        A sequence of pairs of new column names and :class:`.Computation`
        instances.
    :param replace:
        If :code:`True` then new column names can match existing names, and
        those columns will be replaced with the computed data.
    :returns:
        A new :class:`.Table`.
    """
    column_names = list(copy(self._column_names))
    column_types = list(copy(self._column_types))

    for new_column_name, computation in computations:
        new_column_type = computation.get_computed_data_type(self)

        if new_column_name in column_names:
            if not replace:
                raise ValueError(
                    'New column name "%s" already exists. Specify replace=True to replace with computed data.'
                )

            i = column_names.index(new_column_name)
            column_types[i] = new_column_type
        else:
            column_names.append(new_column_name)
            column_types.append(new_column_type)

        computation.validate(self)

    new_columns = OrderedDict()

    for new_column_name, computation in computations:
        new_columns[new_column_name] = computation.run(self)

    new_rows = []

    if replace:
        for i, row in enumerate(self._rows):
            # Slow version if using replace
            values = []

            for j, column_name in enumerate(column_names):
                if column_name in new_columns:
                    values.append(new_columns[column_name][i])
                else:
                    values.append(row[j])
        new_rows.append(Row(values, column_names))
    else:
        def process_row(i, row):
            values = row.values() + tuple(c[i] for c in new_columns.values())

            return Row(values, column_names)

        pool = Pool()
        new_rows = pool.map(process_row, enumerate(column_names))


    return self._fork(new_rows, column_names, column_types)









