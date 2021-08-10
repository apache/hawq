"always raise an exception and make sure pychecker doesn't - from Greg Ward"

class MyError(Exception):
    'd'
    def __init__(self, msg):
        self.msg = msg

raise MyError("FOO!")

