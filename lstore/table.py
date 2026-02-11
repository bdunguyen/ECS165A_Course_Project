from lstore.index import Index
from time import time
from lstore.page import Page

RID_COLUMN = 0
INDIRECTION_COLUMN = 1
SCHEMA_ENCODING_COLUMN = 2
TIMESTAMP_COLUMN = 3
USER_COLUMN_START = 4


class Record:

    def __init__(self, rid, indirection, se, *columns):
        self.rid = rid # tuple of coordinates (col_no, page_no, index)
        self.indirection = indirection 
        self.se = se
        self.columns = columns # column data 
        

class Table:

    """ 
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key 
        
        self.b_pages_dir = {i: [Page()] for i in range(num_columns)} # base page directory
        self.t_pages_dir = {i: [] for i in range(num_columns)} # tail page directory
        
        self.num_columns = num_columns # this would be the number of pages
        self.index = Index(self)
        
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        


        pass

    def __merge(self):
        print("merge is happening")
        pass

'''
t = Table('test', 5, 0)
print(t.index.indices)
'''


