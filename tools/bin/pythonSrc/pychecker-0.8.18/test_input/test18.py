'd'

class A:
    "d"
    def __init__(self): pass

class X:
    "d"
    def __init__(self, x): pass

class Y:
    "d"
    def __init__(self, x, y, z): pass

class Z:
    "d"
    def __init__(self, x, y, z, *args, **kw): pass

def j():
    A()
    A(1)

    X()
    X(1)
    X(1, 2)
    X(1, 2, 3)
    X(1, 2, 3, 4)

    Y()
    Y(1)
    Y(1, 2)
    Y(1, 2, 3)
    Y(1, 2, 3, 4)

    Z()
    Z(1)
    Z(1, 2)
    Z(1, 2, 3)
    Z(1, 2, 3, 4)
    Z(1, 2, 3, 4, 5)
    Z(1, 2, 3, 4, 5, 6)
    Z(1, 2, 3, 4, k=1, j=2, m=3, f=5)


