'd'

from types import StringType, NoneType
from string import upper

def foo():
    return map(lambda x: upper(x), 'testing')

def func(a, b, *args, **kw):
    'verify no warnings for variables (arguments) used before set'
    print a, b, args, kw

class E(Exception):
    'doc'


def x():
    'instantiate a new E with many args, should not be a warning'
    print E('test', 'test', 'test', 'test', 'test', 0)

def y():
    from string import lower
    lower = lower('Unknown option')

def foobar(x): return x

def nn():
    print map(lambda x, s=foobar: s(x), [1,2])

def start() :
    int(args=(lambda x=None: x,))

def nn2():
    n = []
    n.append(1)
    n.append(1, 2)
    n.append((1, 2))

def run():
    foobar(x={0:5})
