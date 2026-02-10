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
        pass

    
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
    def getRID(self):
        RID = []
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
                
        
        return RID
        

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        RID_COLUMN = 0
        INDIRECTION_COLUMN = 1
        SCHEMA_ENCODING_COLUMN = 2
        
        try:
            if len(columns) != self.table.num_columns:
                return False
            
            primary = columns[self.table.key]
            if primary in self.table.page_directory:
                return False
        
            indirection = 0
            schema_encoding = 0    
            rid_value = primary


            RID_list = self.getRID()

            for i in range(len(RID_list)):
                k, pg_no, rec_no = RID_list[i]
                page = self.table.b_pages_dir[k][pg_no]

                if k ==RID_COLUMN:
                    value = rid_value
                elif k ==INDIRECTION_COLUMN:
                    value = indirection
                elif k ==SCHEMA_ENCODING_COLUMN:
                    value = schema_encoding
                else:
                    value = columns[k -4]

                if not page.write(value):
                    return False
            
            self.table.page_directory[primary] = RID_list

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
        try:
            total = 0 # initialize sum
            in_record = False # set in_record to false

            for record in self.table.page_directory.values(): # go through page in table
                key = record["columns"][self.table.key] # find key
                if start_range <= key <= end_range: # if key is within range
                    value = record["columns"][aggregate_column_index] # set value
                    if value is not None: # if value not empty
                        total += value # add value to total
                    in_record = True # change in_record to true

                if not in_record:
                    return False
                return total
            
        except Exception:
            return False

        total = 0
        in_record = False
    
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
        pass

    
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
