'd'

__pychecker__ = 'maxargs=4 maxlocals=3'

def A(a, b, c): pass
def B(a, b, c, d): pass
def C(a, b, c, d, e): pass
def D(a, b, c, d, f, g): pass

def E(a, b, c, d):
    w = x = y = 0
    print a, b, c, d, w, x, y

def F(a, b, c, d):
    w = x = y = z = 0
    print a, b, c, d, w, x, y, z

def G(a, b, c, d):
    w = x = y = z = jj = 0
    print a, b, c, d, w, x, y, z, jj

