'test abstract classes'

__pychecker__ = 'no-classdoc'

class Abstract:
    def f(self): raise NotImplementedError, "override in subclass"
    def g(self): raise NotImplementedError
class ConcreteBad(Abstract):
    def g(self): pass
class ConcreteGood(ConcreteBad):
    def f(self): pass
a = Abstract()                          # error
cb = ConcreteBad()                      # error
cg = ConcreteGood()                     # ok, f defined

class ConcreteInst:
    def f(self): raise SystemError("not yet ready for prime time")
ch = ConcreteInst()                     # ok, raises SystemError

class AbstractInst:
    def f(self): raise NotImplementedError("not yet ready for prime time")
ch = AbstractInst()

cb.g()
def f():
    class MoreBad(Abstract):
        def g(): pass
    _ = MoreBad()                       # FIXME, error not caught

import import73
class ImplAbstract(import73.AbstractLib):
    def __init__(self):
        import73.AbstractLib.__init__(self)

def lib_example():
    _ = import73.AbstractLib()          # error
    unused = ImplAbstract()             # error
