'd'

def x(): pass
def x(): pass

class X:
   'first use okay'
class X:
   'reuse not okay'

class Y:
    'should warning about x being redefined'
    def x(self): pass
    def x(self): pass

class Z:
    'should not be a warning'
    def x(self): pass
