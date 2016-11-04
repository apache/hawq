
import UserDict 

class jj(UserDict.UserDict) :
    "jj"
    def __init__(self):
      UserDict.UserDict.__init__(self, None)

class jj2(UserDict.UserDict) :
    "jj2"
    def __init__(self):
      "Warning don't call UserDict.UserDict.__init__()"
      print "don't call UserDict.UserDict.__init__()"

class jj3(jj) :
    "jj3"
    def __init__(self):
      jj.__init__(self)

class jj4(jj) :
    "jj4"

class A: 
    'should not generate warnings'
    def __init__(self): pass

class B(A):
    'should not generate warnings'

class C(B):
    'should not generate warnings'
    def __init__(self): B.__init__(self)
