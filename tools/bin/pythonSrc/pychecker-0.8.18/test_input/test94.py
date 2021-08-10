'docstring'

def f(a):
    a = a
    a.b = a.b
    a.b.c = a.b.c
    a.b.c.d = a.b.c.d
    c = a & a
    c = a | a
    c = a.b | a.b
    c = a.b.c | a.b.c
    c = a ^ a
    c = a.b ^ a.b
    c = a.b.c ^ a.b.c
    c = a / a
    print c

def g():
    a = b = c = 1
    a = a / 2.0
    a /= 2.0
    a = a + b
    a = a & b
    a = a << b
    a = b << a
    print c
    a = str(a)
    a = `a`
    a = -a
    a = ~a

def h():
  a = 1
  a += a
  a -= a
  a *= a

  a = not a
  a = not not a

  a = (1, 2, 3)
  a, b, c = a

def i(a):
    z = hash(a.x) & hash(a.y)
    z = hash(a.x) | hash(a.y)
    z = hash(a.x) ^ hash(a.y)
    return z

def j(x, y):
    return (x + 1.0) / (y + 2.0)

def k(d):
    return ((1 << d) | (1 << ((d+4) % 8)))
