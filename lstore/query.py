from lstore.table import Table, Record
from lstore.index import Index
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
        try:

            RIDs= self.table.index.locate(self.table.key, primary_key)
            if not RIDs:
                return False
        
            RID = RIDs[0]
            return bool(self.table.delete_record(RID))
    
        except Exception:
            return False
        
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

            for i in range(len(*columns)): # insertion process
                self.table.b_pages_dir[i][RID[1]][RID[2]] = columns[i]
            
            
            self.table.key_ind(Record(RID, None, 0 * self.table.num_records, *columns))

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
        try:
            RIDs = self.table.index.locate(search_key_index, search_key)

            results = []

            for RIDs, record in self.table.page_directory.items():
                data = record["columns"]

                if data[search_key_index] != search_key:
                    continue

                else: 
                    projected = [] 

                    for i in range(self.table.num_columns):
                        if projected_columns_index[i] == 1:
                            projected.append(data[i]) 


                key = data[self.table.key]
                results.append(Record(RIDs, key, projected))

            return results
        
        except Exception:
            return False


    
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
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        try:
            # find base record
            for RID, record in self.table.page_directory.items(): # go through page
                if record["columns"][self.table.key] == primary_key: # if primary key match table key
                    for i in range(self.table.num_columns): # go through column
                        if columns[i] is not None: # if column not empty
                            record["columns"][i] = columns[i] # update column
                    return True
            return False
            
        except Exception: # if no record return false
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
        # NOTE: indirection of base page points to the most recent tail page of that record

        if relative_version > 0: return False

        # create a key for the rids..
        # self.table.index.key_index(0) # this will make the rids hashable

        # variable to store the total sum
        res = 0

        # 1. we must find each record in the base pages.
            # - start range: primary key is equal to start_range
            # - end range: primary key is equal to end_range
        for key in range(start_range, end_range + 1):
            # a) we look for each key in our base page dir for the primary key col
            base_record = self.table.index.indices[self.table.key][start_range] # this gives us the whole base record

            # b) find the RID of the base page
            base_rid = base_record.rid

            cur_record = base_record # we initialize a cur_record
            relative_version_copy = relative_version

            # c) while relative version <= 0: check there is a version to go back to, go to indirection, go to that rid (keep track of where this is), increase relative version by 1
                # RID_COLUMN = 0
                # INDIRECTION_COLUMN = 1
                # we can store the base page RID and if we ever run into it again in the indirection col, we know that is that last version.
            while relative_version_copy <= 0:
                indirection_rid = cur_record.indirection.rid
                if indirection_rid == None or indirection_rid == base_rid:
                    # this is the record that we want
                    # add the value of the record in the specified column
                    res += cur_record.columns[aggregate_column_index]
                    # exit the loop
                    break
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
