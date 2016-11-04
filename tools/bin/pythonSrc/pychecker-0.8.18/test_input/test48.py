'd'

class X:
    'd'
    def x(self): pass
    def y(self): pass

def x():
    i, l, d, s, n, e = 0, [], {}, '', None, Ellipsis
    i.has_no_method()
    l.has_no_method()
    d.has_no_method()
    s.has_no_method()
    n.has_no_method()
    e.has_no_method()

    print i.has_no_attr
    print l.has_no_attr
    print d.has_no_attr
    print s.has_no_attr
    print n.has_no_attr
    print e.has_no_attr

    l.sort()
    d.clear()

    d[i] = jj = X()
    jj.a()
    jj.b()
    jj.x()
    jj.y()

def y():
    for j in []:
        print j.a()


class Z:
    'd'
    def __init__(self): pass
    def x(self): pass
    def y(self): pass

def zz():
    tt = Z()
    tt.a()
    tt.b()
    tt.x()
    tt.y()

def abc(a, b, c):
    # these should all generate a warning for doing nothing
    a
    b
    c
    abc
    5
    zz.func_doc
    str

def j343():
    d = {}
    d['HOME']
    l = []
    l[5]
    d, l

def jklasdflksdjf():
  d = {}
  5 + 3
  5 - 3
  5 / 3.
  5 * 3
  5 ** 3
  d.get
  d[1]
  d[1:2]
  d, d
  [d, d]

def print_item_to(fp, boundary):
    # this should not generate a warning, note comma at end
    print >> fp, '\n--' + boundary + '--',

