
# ALL CODE HERE WRITTEN SPECIFICIALLY FOR INTEGER TYPE 0-255.
class Page:

    def __init__(self): 
        self.num_records = 0
        self.data = bytearray(4096)
        self.curr = 0

    def has_capacity(self, value): # checks if the page is full
        space = len(self.data)
        if space >= self.curr: 
            return True
        else: 
            return False

    def write(self, value):
        if self.has_capacity(value):
            self.data[self.curr] = value

            return True
        else:
            return False
        
  