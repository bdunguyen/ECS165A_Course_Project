from index import Index
from time import time
from page import Page
from collections import defaultdict

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record: # used in insert operations

    def __init__(self, rid, key, columns):
        self.rid = rid # row identifier
        self.key = key # the value on that record's primary key column
        self.columns = columns # no. of cols

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key # the column which is the primary key
        self.num_columns = num_columns # this would be the number of pages
        self.b_page_directory = defaultdict(int) # maps page ids to locations of the page
        self.index = Index(self)
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        self.b_page_no = 0
        self.t_page_directory = defaultdict(int)
        self.t_page_no = 0

        for i in range(num_columns): # creates initial base pages for each column
            self.b_page_directory[i]= Page()
            self.b_page_no += 1 # keep track of no of base pages 

    def __merge(self):
        print("merge is happening")
        pass
 

t = Table('test', 3, 0)
print(t.b_page_directory)
print(t.b_page_no)

