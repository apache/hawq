
import imp
import re

_NOT_USED1 = None
NOT_USED2 = None

def getModules(n = 0, j = 3) :
    "Return list of modules by removing .py from each entry."
    for m in unknownList :
        pass

def jjj(self) :
    import sys
    print sys.argv

def jjj2(a, b = None, c = None) :
    pass

def ddd(a, *args):
    jjj2()
    jjj2(1)
    jjj2(1, 2)
    jjj2(1, 2, 3)
    jjj2(1, 2, 3, 4)

def ppp():
   ddd()
   ddd(1)
   ddd(1, 2)
   ddd(1, 2, 3)

class X:
    def __init__(self) :
        print sys.argv
        print self.__class__, self.__doc__, self.__dict__, self.__x__, self.x
        self.y = 0
        print self.y
    def r(sf):
        nofunc()
        for i in range(0, len(self.a)) :
            jjj(1, 2, 3, k=5)

class Y(X):
    def xxx(self, adf) :
        asdf = adf
        # jjjj doesn't exist
        j = jjjj
        # z() doesn't exist
        self.z()
        adf.mmm.kkk()
        # j doesn't exist
        self.j = 0
        self.xxx(1, 2)

def uuu():
  a=1+2
  return a[0], [['a', 'b'], [1,2,3,4]]

def renderElement(a, b=2, c=3, **kw):
    contents = kw
    print contents

def renderItem(text):
    return renderElement(1, contents=text, z=1, y=2, x=3)

def test_kw_lambda(a, b, c):
    return renderElement(a, ff=lambda value: '=' + renderItem(b))

str = '53'
_ = "blah, don't care, shouldn't warn"

def getOneImageFromPath(k, v):
    d = []
    d.append(id=k, original_flag=v)
