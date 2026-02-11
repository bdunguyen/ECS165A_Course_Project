class Page:

    def __init__(self): 
        self.num_records = 0
        self.data = bytearray(4095) # each page is 4.095 KB, each slot is 5 bytes, so there should be 819 slots per page
        self.curr = 0
        self.length = 5 # size of a slot

    def has_capacity(self, value): # checks if the page is full
        space = len(self.data)
        
        if type(value) == int:
            if space >= self.curr + self.length: # checks if there is enough space
                return True
            else: 
                return False
        else:
            raise Exception('Integer data only.')

    def write(self, value):

        if self.has_capacity(value):
            self.data[self.curr: self.curr + self.length]=  value.to_bytes(self.length, "big") # fills in the bytes accordingly
            self.curr += self.length # adjusts the current free spot
            self.num_records += 1
            return True
        else:
            return False
    
    # chunk may be used when we want to search, locate, do operations...
    def chunk(self): # chunks bytearray to ensure reading only one data (a chunk of 5 bytes); indicates the "slot" the data is in
        for i in range(0, len(self.data), self.length):
            yield self.data[i: i+ self.length] # generates all the chunks


