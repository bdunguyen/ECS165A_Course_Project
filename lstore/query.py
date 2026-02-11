from lstore.table import Record
from lstore.page import Page
from time import time


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
            base_rec = self.table.index.locate(self.table.key, primary_key)
            if not base_rec:
                return False

            rid = self.assignRID('t')

            for i in range(self.table.num_columns):
                page = self.table.t_pages_dir[i][rid[i][1]]
                page.write(0) # for null
            return True
            
            
    

    def assignRID(self, type): # find the next available space to add data for a whole record
        RID = []
        if type == 'b':
            for k,v in self.table.b_pages_dir.items():
                pg_no = len(v) -1
                pg_rec_no = v[pg_no].num_records
                if pg_rec_no < 819:
                    RID.append((k,pg_no,pg_rec_no))
                else:
                    self.table.b_pages_dir[k].append(Page())
                    pg_no = len(v) -1
                    pg_rec_no = v[pg_no].num_records
                    RID.append((k,pg_no,pg_rec_no))
        elif type == 't':
            for k,v in self.table.t_pages_dir.items():
                pg_no = len(v) -1
                pg_rec_no = v[pg_no].num_records
                if pg_rec_no < 819:
                    RID.append((k,pg_no,pg_rec_no))
                else:
                    self.table.t_pages_dir[k].append(Page())
                    pg_no = len(v) -1
                    pg_rec_no = v[pg_no].num_records
                    RID.append((k,pg_no,pg_rec_no))            
                
        
        return RID # return the full RID

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        try: 
            RID = self.assignRID('b') # assign RID to the new entry
            # print(RID)

            for i in range(len(columns)): # insertion process
                page = self.table.b_pages_dir[i][RID[i][1]]

                value = columns[i]

                page.write(value)
            
            self.table.index.key_index(Record(RID, None, [0] * self.table.num_columns, *columns), self.table.key)

            return True 
        except:
            return False

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        return self.select_version(search_key, search_key_index, projected_columns_index, 0)


    
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
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        if relative_version > 0: return False
        res = [] # a list of Record objects upon success

        if search_key not in self.table.index.indices[search_key_index]:
            return False
        
        # print("search_key_index:", search_key_index)
        # print("search_key:", search_key)

        # print("self.table.index.indices[search_key_index]:", self.table.index.indices[search_key_index][search_key])


        base_record = self.table.index.indices[search_key_index][search_key] # this gives us the whole base record


        #print("base:", base_record.columns)

        if base_record.indirection == None:
            print(base_record.columns)
            res.append(base_record)
            return res
        else:
            cur_record = base_record.indirection
        
        relative_version_copy = relative_version

        while relative_version_copy < 0:
            print("hi")
            if cur_record.indirection == None or cur_record == base_record:
                break
            # otherwise, we go to the location of the indirection rid
            # we look at where our record is located
            cur_record = cur_record.indirection
            # update relative version
            relative_version_copy += 1

        # return_record_cols = [cur_record.columns[i] if projected_columns_index[i] == 1 else None for i in range(len(projected_columns_index))] 
        # return_record = Record(cur_record.rid, cur_record.indirection, cur_record.se, return_record_cols)

        #print("cur:", cur_record.columns)

        res.append(cur_record)
        
        return res

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        try:
            if primary_key not in self.table.index.indices[self.table.key]:
                return False
            
            base_record = self.table.index.indices[self.table.key][primary_key] # this gives us the whole base record


            # we need the latest record
            if base_record.indirection:
                latest_record = base_record.indirection
            else:
                latest_record = base_record
            
            # build the latest tail record
            se = []
            new_columns = []

            for i, prev_col in enumerate(latest_record.columns):
                # print("i:", i)
                # print("latest_record.columns:", latest_record.columns)
                if columns[i] == None:
                    se.append(0)
                    new_columns.append(prev_col)
                else:
                    se.append(1)
                    new_columns.append(columns[i])

            # print("SE:", se)
            # print("New Cols:", new_columns)

            RID = self.assignRID('t')

            # print('lastest_record:', latest_record.columns)

            latest_tail_record = Record(RID, None, se, *new_columns)
            latest_tail_record.indirection = latest_record

            # print("lasest tail rec indirection:", latest_tail_record.indirection.columns)

            base_record.indirection = latest_tail_record

            # print(latest_tail_record.columns)

            # now we need to take care of the data stored in the page directories
            # for each column, there is a key in tail page directory
            for col_num, pages in self.table.t_pages_dir.items:
                # we want to look at the last page
                # see if there is space on that page
                value = columns[col_num]
                if pages[-1].has_capacity(value):
                    pages[-1].write(value)
                else:
                    new_page = Page()
                    new_page.write(value)
                    self.table.t_pages_dir[col_num].append(new_page)

            return True

        except Exception:
            return False
        
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):

        return self.sum_version(start_range, end_range, aggregate_column_index, 0)
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):

        if relative_version > 0: return False

        # variable to store the total sum
        res = 0

        # 1. we must find each record in the base pages.
            # - start range: primary key is equal to start_range
            # - end range: primary key is equal to end_range
        for key in range(start_range, end_range + 1):
            # a) we look for each key in our base page dir for the primary key col
            if key not in self.table.index.indices[self.table.key]:
                continue
            
            base_record = self.table.index.indices[self.table.key][key] # this gives us the whole base record

            # b) find the RID of the base page
            base_rid = base_record.rid

            if base_record.indirection == None:
                cur_record = base_record # we initialize a cur_record
            else:
                cur_record = base_record.indirection
            
            relative_version_copy = relative_version

            # c) while relative version <= 0: check there is a version to go back to, go to indirection, go to that rid (keep track of where this is), increase relative version by 1
                # RID_COLUMN = 0
                # INDIRECTION_COLUMN = 1
                # we can store the base page RID and if we ever run into it again in the indirection col, we know that is that last version.
            while relative_version_copy <= 0:
                if cur_record.indirection == None or cur_record.rid == base_rid:
                    # this means this is the base record
                    # print("cur_record.columns[aggregate_column_index]:", cur_record.columns[aggregate_column_index])
                    res += cur_record.columns[aggregate_column_index]
                    # print("cur res:", res)
                    # print("value to add:", cur_record.columns[aggregate_column_index])
                    # exit the loop
                    break

                #print("cur_record.columns[aggregate_column_index]:", cur_record.columns[aggregate_column_index])
                # otherwise, we go to the location of the indirection rid
                # we look at where our record is located
                cur_record = cur_record.indirection
                # update relative version
                relative_version_copy += 1
            
        return res

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False



from lstore.db import Database

from random import choice, randint, sample, seed

db = Database()
# Create a table  with 5 columns
#   Student Id and 4 grades
#   The first argument is name of the table
#   The second argument is the number of columns
#   The third argument is determining the which columns will be primay key
#       Here the first column would be student id and primary key
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1
number_of_aggregates = 1
seed(3562901)

for i in range(0, number_of_records):
    key = 92106429 + randint(0, number_of_records)

    #skip duplicate keys
    while key in records:
        key = 92106429 + randint(0, number_of_records)

    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    print('inserted', records[key])
print("Insert finished")

# Check inserted records using select query
for key in records:
    print('select on', key, ':', records[key])

# Check inserted records using select query
for key in records:
    # select function will return array of records 
    # here we are sure that there is only one record in t hat array
    # check for retreiving version -1. Should retreive version 0 since only one version exists.
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    else:
        pass
        # print('select on', key, ':', record)

updated_records = {}
for key in records:
    updated_columns = [None, None, None, None, None]
    updated_records[key] = records[key].copy()
    for i in range(2, grades_table.num_columns):
        # updated value
        value = randint(0, 20)
        updated_columns[i] = value
        # update our test directory
        updated_records[key][i] = value
    print("Update: ", updated_columns)
    query.update(key, *updated_columns)

    #check version -1 for record
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0]
    error = False
    for j, column in enumerate(record.columns):
        print("j:", j)
        if column != records[key][j]:
            print("record:", record.columns) # we have
            print('column:', column, 'records:', records[key][j])
            error = True
    if error:
        print('update error on', records[key], 'and', updated_columns, ':', record.indirection.columns, ', correct:', records[key])
    else:
        pass
        # print('update on', original, 'and', updated_columns, ':', record)
    print("-1 Done")

    #check version -2 for record
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], -2)[0]
    error = False
    for j, column in enumerate(record.columns):
        if column != records[key][j]:
            error = True
    if error:
        print('update error on', records[key], 'and', updated_columns, ':', record.columns, ', correct:', records[key])
    else:
        pass
        # print('update on', original, 'and', updated_columns, ':', record)
    print("-2 Done")
    
    #check version 0 for record
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0]
    error = False
    for j, column in enumerate(record.columns):
        if column != updated_records[key][j]:
            error = True
    if error:
        print('update error on', records[key], 'and', updated_columns, ':', record.columns, ', correct:', updated_records[key])
    print("0 Done")

'''
keys = sorted(list(records.keys()))
# aggregate on every column 
for c in range(0, grades_table.num_columns):
    for i in range(0, number_of_aggregates):
        r = sorted(sample(range(0, len(keys)), 2))
        # calculate the sum form test directory
        # version -1 sum
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum_version(keys[r[0]], keys[r[1]], c, -1)
        if column_sum != result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
        else:
            pass
            # print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
        # version -2 sum
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum_version(keys[r[0]], keys[r[1]], c, -2)
        if column_sum != result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
        else:
            pass
        # version 0 sum
        updated_column_sum = sum(map(lambda key: updated_records[key][c], keys[r[0]: r[1] + 1]))
        updated_result = query.sum_version(keys[r[0]], keys[r[1]], c, 0)
        if updated_column_sum != updated_result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', updated_result, ', correct: ', updated_column_sum)
        else:
            pass

'''