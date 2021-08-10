"doc"

def xx(**kw):
    return None

def yy():
    c = yy
    g, h = 1, 2
    xx(a='', b=c.a.d.f, e=5, jj=xx(j=5), f=(g+h), k=("%s" % "df"))

def zz():
    b = zz
    xx(b=(1, 2, 3), c={ 'a': b}, d=[1, 2, 3], k=(1 < 2))

def aa(obj):
    print obj.x.y
    print obj.x.y.z
    print obj.x.y.z.a
    print obj.x.y.z.a.b
    print obj.x.y.z.a.b.c

class Obj:
    'd'
    def __init__(self, xx):
        self.xx = xx
        self.xx.y = 0
        self.xx.y.z = 0
        self.xx.y.z.a = 0
        self.xx.y.z.a.b = 0
        self.xx.y.z.a.b.c = 0

    def prn(self):
        print self.xx
        print self.xx.y
        print self.xx.y.z
        print self.xx.y.z.a
        print self.xx.y.z.a.b
        print self.xx.y.z.a.b.c

