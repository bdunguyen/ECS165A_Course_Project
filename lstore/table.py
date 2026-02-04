from lstore.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

from page import Page
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
        self.page_directory = {} # tuple? the (index, page object)?
        self.index = Index(self)
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        
        # pre-allocate for base and tail pages upon initialization
        self.bases = {f'b{i}': Page() for i in range(num_columns)}
        self.tails = {f't{i}': Page() for i in range(num_columns)}

    def __merge(self):
        print("merge is happening")
        pass
 
