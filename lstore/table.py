from index import Index
from time import time
from page import Page

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns # this would be the number of pages
        self.index = Index(self)
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        
        self.b_pages_dir = {i:[Page()] for i in range(num_columns + 4)} # base page directory
        self.t_pages_dir = {i: [] for i in range(num_columns + 4)} # tail page directory

        pass

    def __merge(self):
        print("merge is happening")
        pass

