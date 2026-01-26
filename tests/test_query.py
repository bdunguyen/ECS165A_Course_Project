"""
# Creates a Query object that can perform different queries on the specified table 
Queries that fail must return False
Queries that succeed should return the result or True
Any query that crashes (due to exceptions) should return False
"""
def test_create_query():
    pass


"""
# internal Method
# Read a record with specified RID
# Returns True upon succesful deletion
# Return False if record doesn't exist or is locked due to 2PL
"""
def test_delete():
    # delete(primary_key)
    pass


"""
# Insert a record with specified columns
# Return True upon succesful insertion
# Returns False if insert fails for whatever reason
"""
def test_insert():
    # insert(*columns)
    pass


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
    pass


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
def test_select_version():
    # select_version(search_key, search_key_index, projected_columns_index, relative_version)
    pass


"""
# Update a record with specified key and columns
# Returns True if update is succesful
# Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
"""
def test_update():
    # update(primary_key, *columns)
    pass


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
def test_sum_version():
    # sum_version(start_range, end_range, aggregate_column_index, relative_version)
    pass
