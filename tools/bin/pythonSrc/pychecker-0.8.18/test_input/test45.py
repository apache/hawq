'd'

__pychecker__ = 'argsused'

def x(empty, unused):
    print 'nothing'
def y(empty1, unused1):
    print 'nothing'
def z():
    empty = 1
    unused = 2
    empty1 = 1
    unused1 = 2

class A:
    'd'
    def x(self, other, empty, unused):
        print 'nothing'
    def y(self, other1, empty1, unused1):
        print 'nothing'

def a():
  _ = 5
