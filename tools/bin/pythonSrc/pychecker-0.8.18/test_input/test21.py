'test inconsistent return types'

a, b = 0, 0

def f(z):
    'should be a warning'
    if z:
        return 1
    return  []

def g(z):
    'should be a warning'
    if z:
        return (1, 2)
    return  []

def h(z):
    'should be a warning'
    if z:
        return {}
    return  []

def y():
    'should not be a warning'
    if a: return 1
    elif b: return 0
    else : return cmp(a, b)

def z():
    'should not be a warning'
    if b: return b, a, globals()
    return 1, 2, 3

class A:
    'test returning base class'

class B(A):
    'test returning derived class'

def x(num):
    if num == 1:
        return A()
    if num == 2:
        return B()
    return None

class C:
    'test returning "inconsistent" types from __getattr__ which is allowed'
    def __getattr__(self, attr):
        if attr == 'a':
            return 1
        if attr == 'b':
            return 3.1415926
        if attr == 'b':
            return 'string'
        raise AttributeError, 'no attr ' + attr + ' is a bad attr'

