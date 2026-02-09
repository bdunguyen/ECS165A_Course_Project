class Page:

    def __init__(self): 
        self.num_records = 0
        self.data = bytearray(4096) # each page is 4 KB
        self.curr = 0

    def has_capacity(self, value): # checks if the page is full
        space = len(self.data)
        length = 4
        
        if type(value) == int:
            if space >= self.curr + length: # checks if there is enough space
                return True
            else: 
                return False

    def write(self, value):
        length = 4

        if self.has_capacity(value):
            self.data[self.curr: self.curr + length]=  value.to_bytes(length, "big") # fills in the bytes accordingly
            self.curr += length # adjusts the current free spot
            
            return True
        else:
            return False


### TEST    
t = Page()
t.write(906659671 + 10000)
print(int.from_bytes(t.data[0:4], "big"))
