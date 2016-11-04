__doc__ = 'Auxiliary file for test98.py'

class BaseGood:
    'Nice base init'
    def __init__(self):
        return

class BaseBad:
    'Error should print as coming from this file'
    def __init__(self):
        return 42
