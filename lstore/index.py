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
        
        val = self.indices[column][value]

        if val in self.indices: # checks if the key exists
            return val # returns the RID, which is a list of values pointing to appropriate locations
        else:
            return -1

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin:int, end:int, column):
        RIDs = []
        
        for i in range(begin, end + 1): # performs locate for each value from begin to end + 1
            RIDs.append(self.locate(column, i))

        return RIDs

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):

        if self.indices[column_number] != None:
            # raise error
            return -1
        
        index_dict = defaultdict(list) # initialize a default dict that fills up as data is added
        

        self.indices[column_number] = index_dict

        

        # TODO: Add creating index from table with values already in it later?

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
