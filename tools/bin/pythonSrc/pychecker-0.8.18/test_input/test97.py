# -*- encoding: latin-1 -*-

__doc__ = 'nested scopes test'

u"Blåbærgrød".upper()

try:
    from cStringIO import StringIO
except ImportError:
    # this shouldn't cause a warning about catching string exceptions
    pass

def call(proc, y):
    proc(y)

def fun():
    def setfooattr(x, y):
        call(lambda z: setattr(x, 'foo', z), y)

    setfooattr(Exception(), 'bar')
