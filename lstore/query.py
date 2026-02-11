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

        try:
            res = [] # a list of Record objects upon success

            if search_key not in self.table.index.indices[search_key_index]:
                raise Exception
            
            base_record = self.table.index.indices[search_key_index][search_key] # this gives us the whole base record

            base_rid = base_record.rid

            cur_record = base_record # we initialize a cur_record
            relative_version_copy = relative_version

            while relative_version_copy <= 0:
                if cur_record.indirection == None:
                    break
                elif cur_record.indirection.rid == base_rid:
                    cur_record = cur_record.indirection
                    # we have reached the record we want
                    break
                # otherwise, we go to the location of the indirection rid
                # we look at where our record is located
                cur_record = cur_record.indirection
                # update relative version
                relative_version_copy += 1

            # return_record_cols = [cur_record.columns[i] if projected_columns_index[i] == 1 else None for i in range(len(projected_columns_index))] 
            # return_record = Record(cur_record.rid, cur_record.indirection, cur_record.se, return_record_cols)

            res.append(cur_record)
            
            return res

            # ----

            rec = self.table.index.indices[search_key_index][search_key] # gives record

            print(rec)
            rec = rec.indirection # nav to latest tail

            print(rec)

            while relative_version < 0:
                rec = rec.indirection
                relative_version += 1

                rec.columns = [rec.columns[i] if projected_columns_index[i] == 1 else None for i in range(len(projected_columns_index))] 
            return [rec]
        
        except Exception:
            return False

    
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

            latest_tail_record = Record(RID, latest_record, se, *new_columns)

            base_record.indirection = latest_tail_record

            # print(base_record.indirection.columns)

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

                print("cur_record.columns[aggregate_column_index]:", cur_record.columns[aggregate_column_index])
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
    