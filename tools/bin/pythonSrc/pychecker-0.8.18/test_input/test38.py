'd'

class X:
    'd'
    def m1(self, a, b, c, d): pass
    def m2(self, a, b, c, d, *args): pass
    def m3(self, a, b, c, d, *args, **kwArgs): pass
    def m4(self, a, b=0, c=0, d=0): pass
    def m5(self, a, b=0, c=0, d=0, *args): pass
    def m6(self, a, b=0, c=0, d=0, *args, **kwArgs): pass

class Y1(X):
    'should not generate any warnings'
    def m1(self, a, b, c, d): exec a in globals()
    def m2(self, a, b, c, d, *args): exec a in globals()
    def m3(self, a, b, c, d, *args, **kwArgs): exec a in globals()
    def m4(self, a, b, c, d): exec a in globals()
    def m5(self, a, b, c, d, *args): exec a in globals()
    def m6(self, a, b, c, d, *args, **kwArgs): exec a in globals()

class Z1(X):
    'should generate warnings'
    def m1(self, a, b, c): pass
    def m2(self, a, b, c, d): pass
    def m3(self, a, b, c, d, **kwArgs): pass
    def m4(self, a, b=0, c=0): pass
    def m5(self, a, b=0, c=0, d=0): pass
    def m6(self, a, b=0, c=0, d=0, **kwArgs): pass

class Z2(X):
    'should generate warnings'
    def m3(self, a, b, c, d, *args): pass
    def m6(self, a, b=0, c=0, d=0, *args): pass

class Z3(X):
    'should generate warnings'
    def m3(self, a, b, c, d): pass
    def m6(self, a, b=0, c=0, d=0): pass

class Z4(X):
    'should generate warnings'
    def m1(self): pass
    def m2(self): pass
    def m3(self): pass
    def m4(self): pass
    def m5(self): pass
    def m6(self): pass

class AAA:
    'should generate 1 warning'
    __pychecker__ = 'exec'
    def m1(self, a, b, c, d): pass
    def m2(self, a, b, c, d, *args): exec a in d
    def m3(self, a, b, c, d, *args): exec a
