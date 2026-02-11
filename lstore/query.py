from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page
from time import time
from lstore.table import INDIRECTION_COLUMN, SCHEMA_ENCODING_COLUMN


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
            # record must exist
            if primary_key not in self.table.page_directory:
                return False

            base_rid = self.table.page_directory[primary_key]

            # mark indirection as -1
            ind_page, ind_slot = base_rid[INDIRECTION_COLUMN]
            self.table.b_pages_dir[INDIRECTION_COLUMN][ind_page].data[
                ind_slot*5:(ind_slot+1)*5
            ] = (-1).to_bytes(5, "big", signed=True)

            return True

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
                self.table.b_pages_dir[i][RID[1]][RID[2]]
            
            
            self.table.key_ind(Record(RID, None, 0 * self.table.num_records, *columns))

            self.table.page_directory[columns[self.table.key]] = RID # add RID to page_directory so update() and delete() can access

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

        try:
            rec = self.table.index.indices[search_key_index][search_key] # gives record
            rec = rec.indirection # nav to latest tail

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
            # base RID must exist in page directory
            if primary_key not in self.table.page_directory:    
                return False

            base_rid = self.table.page_directory[primary_key] # get base rid for columns

            # read base indirection
            ind_page, ind_slot = base_rid[INDIRECTION_COLUMN]
            base_ind_page = self.table.b_pages_dir[INDIRECTION_COLUMN][ind_page]
            prev_tail = int.from_bytes(
                base_ind_page.data[ind_slot*5:(ind_slot+1)*5], "big"
            )

            # allocate tail slots
            tail_rid = {}
            for col in range(self.table.num_columns + 4):
                if (
                    len(self.table.t_pages_dir[col]) == 0
                    or self.table.t_pages_dir[col][-1].num_records >= 819
                ):
                    self.table.t_pages_dir[col].append(Page())

                page_no = len(self.table.t_pages_dir[col]) - 1
                slot_no = self.table.t_pages_dir[col][page_no].num_records
                tail_rid[col] = (page_no, slot_no)

            # write tail record
            schema = 0

            for col in range(self.table.num_columns + 4):
                page_no, slot_no = tail_rid[col]
                page = self.table.t_pages_dir[col][page_no]

                if col == INDIRECTION_COLUMN:
                    page.write(prev_tail if prev_tail != 0 else primary_key)

                elif col == SCHEMA_ENCODING_COLUMN:
                    for i, v in enumerate(columns):
                        if v is not None:
                            schema |= (1 << i)
                    page.write(schema)

                elif col < 4:
                    page.write(0)

                else:
                    user_col = col - 4
                    if columns[user_col] is not None:
                        page.write(columns[user_col])
                    else:
                        # copy base value
                        base_page, base_slot = base_rid[col]
                        base_page_obj = self.table.b_pages_dir[col][base_page]
                        old_val = int.from_bytes(
                            base_page_obj.data[base_slot*5:(base_slot+1)*5], "big"
                        )
                        page.write(old_val)

            # update base indirection to newest tail
            base_ind_page.data[ind_slot*5:(ind_slot+1)*5] = \
                primary_key.to_bytes(5, "big")

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
        # NOTE: indirection of base page points to the most recent tail page of that record

        if relative_version > 0: return False

        # create a key for the rids..
        self.table.index.key_index(0) # this will make the rids hashable

        # variable to store the total sum
        res = 0

        # 1. we must find each record in the base pages. 
        for key in range(start_range, end_range + 1):
            # a) we look for each key in our base page dir for the primary key col
            key_col_num, page_index, record_index = self.table.index.indices[self.table.key][key] # gives us a tuple (key_col_num, page, index on page)

            # b) find the RID of the base page
            base_rid = int.from_bytes(self.table.b_pages_dir[1][page_index].data[(record_index * 5) : (record_index * 5) + 5])

            relative_version_copy = relative_version

            # c) while relative version <= 0: check there is a version to go back to, go to indirection, go to that rid (keep track of where this is), increase relative version by 1
                # RID_COLUMN = 0
                # INDIRECTION_COLUMN = 1
                # we can store the base page RID and if we ever run into it again in the indirection col, we know that is that last version.
            while relative_version_copy <= 0:
                indirection_rid = int.from_bytes(self.table.t_pages_dir[1][page_index].data[(record_index * 5) : (record_index * 5) + 5])
                if indirection_rid == None or indirection_rid == base_rid:
                    # this is the record that we want
                    # add the value of the record in the specified column
                    res += int.from_bytes(self.table.t_pages_dir[aggregate_column_index][page_index].data[(record_index * 5) : (record_index * 5) + 5])
                    # exit the loop
                    break
                # otherwise, we go to the location of the indirection rid
                # we look at where our record is located
                key_col_num, page_index, record_index = self.table.index.indices[0][indirection_rid]
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
