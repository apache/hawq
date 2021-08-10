import exceptions

class Data:
    class DataError(exceptions.ValueError): pass
    def __init__(self, value):
        self.value = value
    def get_value(self):
        return self.value
