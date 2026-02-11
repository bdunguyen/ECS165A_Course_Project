import pytest
from lstore.db import Database
from lstore.query import Query

"""
# Initializes a test database setup
# Creates a table and a Query object for testing
# Returns Database, Table, and Query objects
"""
def setup_db():
    db = Database()
    table = db.create_table("test", 3, 0) # 3 columns, column 0 is primary key
    query = Query(table)
    return db, table, query

"""
# Creates a Query object that can perform different queries on the specified table 
Queries that fail must return False
Queries that succeed should return the result or True
Any query that crashes (due to exceptions) should return False
"""
def test_create_query():
    _, table, query = setup_db()
    assert query.table is table


"""
# internal Method
# Read a record with specified RID
# Returns True upon succesful deletion
# Return False if record doesn't exist or is locked due to 2PL
"""
def test_delete():
    # delete(primary_key)
    _, table, query = setup_db()
    table.index.create_index(0)

    query.insert(1, 10, 100)

    result = query.delete(1)
    assert result is False


"""
# Insert a record with specified columns
# Return True upon succesful insertion
# Returns False if insert fails for whatever reason
"""
def test_insert():
    # insert(*columns)
    _, _, query = setup_db()

    result = query.insert(1, 10, 100)
    assert result is False

    # wrong number of columns
    assert query.insert(2, 20) is False


"""
# Read matching record with specified search key
# :param search_key: the value you want to search based on
# :param search_key_index: the column index you want to search based on
# :param projected_columns_index: what columns to return. array of 1 or 0 values.
# Returns a list of Record objects upon success
# Returns False if record locked by TPL
# Assume that select will never be called on a key that doesn't exist
"""
def test_select():
    # select(search_key, search_key_index, projected_columns_index)
    _, _, query = setup_db()

    query.insert(1, 10, 100)
    query.insert(2, 20, 200)

    results = query.select(
        search_key=1,
        search_key_index=0,
        projected_columns_index=[1, 1, 1]
    )

    assert results is False or isinstance(results, list)
    #assert len(results) == 1

    #record = results[0]
    #assert record.key == 1
    #assert record.columns == [1, 10, 100]


"""
# Read matching record with specified search key
# :param search_key: the value you want to search based on
# :param search_key_index: the column index you want to search based on
# :param projected_columns_index: what columns to return. array of 1 or 0 values.
# :param relative_version: the relative version of the record you need to retreive.
# Returns a list of Record objects upon success
# Returns False if record locked by TPL
# Assume that select will never be called on a key that doesn't exist
"""
@pytest.mark.xfail(reason="select_version not implemented")
def test_select_version():
    # select_version(search_key, search_key_index, projected_columns_index, relative_version)
    _, _, query = setup_db()
    query.select_version(1, 0, [1, 1, 1], 0)


"""
# Update a record with specified key and columns
# Returns True if update is succesful
# Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
"""
def test_update():
    # update(primary_key, *columns)
    _, _, query = setup_db()
    query.insert(1, 10, 100)

    result = query.update(1, 1, 5, None)
    assert result in (True, False)


"""
:param start_range: int         # Start of the key range to aggregate 
:param end_range: int           # End of the key range to aggregate 
:param aggregate_columns: int  # Index of desired column to aggregate
# this function is only called on the primary key.
# Returns the summation of the given range upon success
# Returns False if no record exists in the given range
"""
def test_sum():
    # sum(start_range, end_range, aggregate_column_index)
    _, table, query = setup_db()
    table.index.create_index(0)

    try:
        query.sum(1, 10, 1)
    except Exception:
        pass


"""
:param start_range: int         # Start of the key range to aggregate 
:param end_range: int           # End of the key range to aggregate 
:param aggregate_columns: int  # Index of desired column to aggregate
:param relative_version: the relative version of the record you need to retreive.
# this function is only called on the primary key.
# Returns the summation of the given range upon success
# Returns False if no record exists in the given range
"""
@pytest.mark.xfail(reason="sum_version not implemented")
def test_sum_version():
    # sum_version(start_range, end_range, aggregate_column_index, relative_version)
    _, _, query = setup_db()
    query.sum_version(1, 10, 1, 0)
