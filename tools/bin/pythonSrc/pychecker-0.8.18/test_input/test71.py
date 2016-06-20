'test unreachable code'

__pychecker__ = 'unreachable'

def a(x):
    if x == 5:
        return x
    return 0
    print x

def b(x):
    if x == 5:
        return x
    raise ValueError
    print x

def c(x):
    if x == 5:
        return
    return
    print x

def d(x):
    if x == 5:
        return
    raise ValueError
    print x

def e(x):
    def foo():
        print x
    foo()
    return x

def f():
    x = e(1)
    print x

class Point:
    """doc string"""
    parse = None

    def get_position(self, parts):
        try:
            return Point.parse('%s %s' % (parts[1], parts[2]), float(parts[3]))
        except ValueError, e:
            # FIXME: handle better than printing
            print 'Unable to process position report\n', e
        return None

