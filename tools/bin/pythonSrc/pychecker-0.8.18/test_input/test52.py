"doesn't work in 1.5.2"

class foo:
    'd'
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kw = kwargs

class foo2(foo):
    'd'
    def __init__(self, *args):
        foo.__init__(self, jj=5, kk=10, *args)

class foo3(foo):
    'd'
    def __init__(self, **kwargs):
        foo.__init__(self, jj=5, kk=10, **kwargs)

class foo4(foo):
    'd'
    def __init__(self, *args, **kwargs):
        foo.__init__(self, jj=5, kk=10, *args, **kwargs)

class c: 
    'd'
    def __init__(self): 
        pass 

class c1: 
    'd'
    o = c() 
    def c(self,x): 
        print x 

def x():
    a = c1() 
    a.b = c1() 
    # we can't handle this yet
    # a.b.c(5)

class c2: 
    'd'
    def m1(self): 
        print "ok" 
    def m2(self,x): 
        print "ok" 

class c3: 
    'd'
    c2 = None 
    def m(self): 
        self.c2.m1()
        self.c2.m2(5)

