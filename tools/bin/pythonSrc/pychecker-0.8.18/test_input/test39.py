
def func1(): pass
def func2(): pass

class X1:
    def x(self) : pass
    def y(self) : pass

class X2:
    def x(self) : pass
    def y(self) : pass

class Y1:
    def a(self) : pass
    def x(self) : pass
    def y(self) : pass

class Y2:
    def a(self) : pass
    def x(self) : pass
    def y(self) : pass

class check_here:
    'test __pychecker__ var at class scope'
    __pychecker__ = 'funcdoc argsused unusednames=foo,bar'
    def a(self) :
        'd'
        pass
    def x(self, foo) :
        print self
    def y(self, bar) :
        print self
    def z(self, jjj) :
        j = 0

# Requires Python 2.2+
try:
    class NewStyle(object):
        'test that suppressions work with new style classes'
        def f(self):
            i = 5

except NameError:
    pass
