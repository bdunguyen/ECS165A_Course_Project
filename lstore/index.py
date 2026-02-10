from collections import defaultdict
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

    def key_index(self, key_col_no):
        if self.indices[key_col_no] is None: # if no index exists, create one for key column
            for i in range(len(self.table.b_pages_dir[key_col_no])):
                for j in range(819):
                    self.indices[key_col_no] = {int.from_bytes(self.table.b_pages_dir[key_col_no][i].data[j: j + 5], "big"): (key_col_no, i, j)} # assign key value to RID
        else:
            raise Exception('Index already exists.')
    



        # self.indices[key_col_no] = {self.table.b_pages_dir[key_col_no][i]: }

    def locate(self, column: int, value:str) -> list[int]: # column integer
        # Check if valid column
        if column < 0 or column > len(self.indices):
            # raise error
            return -1
        
        # check if we have index
        if self.indices[column] == None:
            return -1 # return empty list because we don't have any rids
        
        return self.indices[column][value] # returns RID tuple

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


