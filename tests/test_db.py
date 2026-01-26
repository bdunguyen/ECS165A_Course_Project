from lstore.db import Database

"""
# Creates a new table
:param name: string         #Table name
:param num_columns: int     #Number of Columns: all columns are integer
:param key: int             #Index of table key in columns
"""
def test_create_table():
    db = Database()

    table1 = db.create_table("table1", 5, 1)

    result = db.get_table("table1")
    assert result == table1

def test_existing_table():
    db = Database()

    # create table 1
    db.create_table("table1", 5, 1)
    # try to insert table 1 again
    table2 = db.create_table("table1", 3, 5)

    assert table2 == None

"""
# Deletes the specified table
"""
def test_drop_table():
    # drop_table(self, name)
    pass

"""
# Returns table with the passed name
"""
def get_table():
    # get_table(self, name)
    pass