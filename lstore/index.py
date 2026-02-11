from lstore.page import Page

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table

        # each index has a dictionary of the column name (key: possible values in that column, value: List:[RIDs])

    """
    # returns the location of all records with the given value on column "column" - RIDs?
    """

    def key_index(self, record, key_col_no): 
        if self.indices[key_col_no] is None:
            self.indices[key_col_no] = {}

        # print("primary key:", record.columns[key_col_no])
        # print("record:", record)
        
        self.indices[key_col_no][record.columns[key_col_no]] = record
        # print(self.indices[key_col_no])


    def locate(self, column: int, value:str) -> list[int]: # column integer
        # Check if valid column
        if column < 0 or column > len(self.indices):
            # raise error
            return -1
        
        # check if we have index
        if self.indices[column] == None:
            return -1 # return empty list because we don't have any rids
        
        return self.indices[column][value].rid # returns RID tuple

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin:int, end:int, column):
        RIDs = []
        
        for i in range(begin, end + 1):
            RIDs.append(self.locate(column, i))

        return RIDs

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number): 
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass


