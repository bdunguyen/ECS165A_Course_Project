import pytest
from lstore.index import Index
from collections import defaultdict

class MockTable:
    def __init__(self, num_columns):
        self.num_columns = num_columns

class MockRecord:
    def __init__(self, rid):
        self.rid = rid

"""
# returns the location of all records with the given value on column "column"
"""
def test_locate():
    # locate(column, value)

    # set up table and index
    table = MockTable(num_columns=3)
    index = Index(table)

    # create index on column 1
    index.indices[1] = {}

    # manually populate index
    index.indices[1][10] = MockRecord([1, 3, 5])
    index.indices[1][20] = MockRecord([2])

    # valid lookup
    assert index.locate(1, 10) == [1, 3, 5]
    assert index.locate(1, 20) == [2]

    # value not present
    try:
        assert index.locate(1, 99) 
    except KeyError:
        pass

    # column without index
    assert index.locate(0, 10) == -1

    # invalid column
    assert index.locate(-1, 10) == -1
    assert index.locate(10, 10) == -1

"""
# Returns the RIDs of all records with values in column "column" between "begin" and "end"
"""
def test_locate_range():
    # locate_range(self, begin, end, column)

    # set up table and index
    table = MockTable(num_columns=2)
    index = Index(table)

    # create index on column 0
    index.indices[0] = {}

    # manually populate index
    index.indices[0][1] = MockRecord([100])
    index.indices[0][2] = MockRecord([101, 102])
    index.indices[0][4] = MockRecord([103])

    # retrieve RIDs for values 1 through 4 in column 0
    result = index.locate_range(begin=1, end=4, column=0)

    # check that locate_range returns the correct RIDs for each value in the range
    assert result == [
        [100], # value 1
        [101, 102], # value 2
        None, # value 3 (missing)
        [103] # value 4
    ]

"""
# optional: Create index on specific column
"""
def test_create_index():
    # create_index(self, column_number)

    # set up table and index
    table = MockTable(num_columns=3)
    index = Index(table)

    # create index on column 2
    index.indices[2] = {}
    assert index.indices[2] is not None
    # check that it can store lists of RIDs
    assert isinstance(index.indices[2], dict)

"""
# optional: Drop index of specific column
"""
def test_drop_index():
    pass
