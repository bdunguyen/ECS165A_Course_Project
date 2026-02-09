from collections import defaultdict

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns

        # each index has a dictionary of the column name (key: possible values in that column, value: List:[RIDs])

    """
    # returns the location of all records with the given value on column "column" - RIDs?
    """

    def locate(self, column: int, value:str) -> list[int]: # column integer
        # Check if valid column
        if column < 0 or column > len(self.indices):
            # raise error
            return -1
        
        # check if we have index
        if self.indices[column] == None:
            return [] # return empty list because we don't have any rids
        
        return self.indices[column][value]

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
        pages = self.table.b_pages_dir[column_number]
        for i in range(len(pages)): # for i in the range of the list that contains all pages of the column
            for j in range(len(pages[i])):
                pass
        # self.indices[column_number] = {j: []}
        

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
