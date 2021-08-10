'd'

import import69

__pychecker__ = 'callattr'
class B:
    'd'
    def __init__(self): pass

class C(B):
    'd'
    __super_init = B.__init__
    def __init__(self):
        self.__super_init()

class D(import69.Alias):
    'd'
    __super_init = import69.Alias.__init__
    def __init__(self):
        self.__super_init()


