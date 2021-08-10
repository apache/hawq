
"doc"

def func(): pass

class X:
    "should only generate a warning w/-A, --callattr cmd line options"
    def __init__(self, other):
        self.func = func
        self.other = other
        self.x = None

    def test(self):
        "self.y should generate a function"
        self.func()
        self.other()
        self.x()
        self.y()
