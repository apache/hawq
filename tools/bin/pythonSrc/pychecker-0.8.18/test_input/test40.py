'd'

__pychecker__ = 'argsused'

def x0():
    print 'should be ok'

def x1(a, b, c, *args, **kw):
    print a, b, c

def x2(a, b, c, *args, **kw):
    print a, b, c, args

def x3(a, b, c, *args, **kw):
    print a, b, c, args, kw

def x4(a, b, c, *args, **kw):
    print 'uh-oh, forgot to use args'

def x5(a, b, c, *args, **kw):
    raise NotImplementedError

def x6(a, b, c, *args, **kw):
    pass

def x7(a, b, c, *args, **kw):
    return None

class X:
    'd'
    def x1(self):
    	print 'hi'
    def x2(self, a, *args, **kw):
    	print 'oops'
