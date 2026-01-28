
class Page:

    def __init__(self): 
        self.num_records = 0
        self.data = bytearray(4096)
        self.curr = 0

    def has_capacity(self, value): # checks if the page is full
        space = len(self.data)
        if space - self.curr - len(value) >= 0: # check if there is enough space
            return True
        else:
            return False

    def write(self, value):
        if self.has_capacity(value):
            for i in range(len(value)):
                self.data[self.curr] = ord(value[i]) # Adds the data in the appropriate place in the bytearray
                self.curr += 1
            self.num_records += 1

            return True
        else:
            return False
    

