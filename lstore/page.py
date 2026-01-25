
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.curr = 0

    def has_capacity(self):
        if self.num_records < len(self.data):
            return True
        else:
            return False

    def write(self, value):
        if self.has_capacity():
            for i in range(len(value)):
                self.data[self.num_records] = ord(i)
                self.curr += 1
            self.num_records += 1

            return "Write successful!"
        else:
            return "Failed to write."


