'doc'

class X:
    'doc'
    def __init__(self):
        self.fff = 0

    def x(self):
        pass

    def y(self):
        'should generate a warning'
        if self.x:
            pass
        if self.x and globals():
            pass
        if globals() and self.x:
            pass

    def z(self):
        'should NOT generate a warning'
        if globals() :
            pass
        if self.x() and self.fff:
            pass
        if self.x() and globals():
            pass
        if globals() and self.x():
            pass
        if self.fff:
            pass
        print self.x
        print self.fff

class Y(X):
    'doc'
    def j(self):
        'should generate a warning'
        if self.x:
            pass
        print self.fff

    def h(self):
        'should NOT generate a warning'
        if self.x():
            pass
        print self.x
        print self.fff
