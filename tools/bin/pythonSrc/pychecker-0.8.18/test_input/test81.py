'test use of freevars and cellvars'

from __future__ import nested_scopes

__pychecker__ = 'classattr'

class Board:
    'base class to test'
    def atomCount(self):
        pass

def func(l):
    pass

class BoardTests:
    'should not generate a warning'
    def testAddAtom(self):
        b = Board()
        b.atomCount()
        func(lambda:b.atomCount())

def q(funcp):
   print funcp()

def f():
   y = 1
   def g():
      return y
   q(g)

