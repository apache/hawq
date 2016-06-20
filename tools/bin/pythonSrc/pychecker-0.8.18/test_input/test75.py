'test slots & property, this will only work in Python 2.2'

class A:
    'warn about using slots in classic classes'
    __slots__ = ('a',)

try:
    class B(object):
        'no warning'
        __slots__ = ('a',)

    class C(object):
        'warn about using empty slots'
        __slots__ = ()

    class D(object):
        "don't warn about using empty slots"
        __pychecker__ = '--no-emptyslots'
        __slots__ = ()

    class E:
        'this should generate a warning for using properties w/classic classes'
        def getx(self):
            print 'get x'
            return 5
        x = property(getx)

    class F(object):
        'this should not generate a warning for using properties'
        def getx(self):
            print 'get x'
            return 5
        x = property(getx)

    class Solution(list):
        'this should not generate a warning or crash'
        def __init__(self):
            pass

    class MethodArgNames:
        'check warnings for static/class methods for first arg name'
        def __init__(self, *args): pass

        # should warn
        def nn(self, *args): pass
        nn = classmethod(nn)

        # should warn
        def mm(self, *args): pass
        mm = staticmethod(mm)

        # should not warn
        def oo(cls, *args): pass
        oo = classmethod(oo)

        # should not warn
        def pp(*args): pass
        pp = staticmethod(pp)

        # should not warn
        def qq(): pass
        qq = staticmethod(qq)

        # should not warn
        def rr(klass, *args): pass
        rr = classmethod(rr)

    class Bug4(object):
        '''doc'''
        def static(arg1, arg2):
            return arg1+arg2
        static = staticmethod(static)

        def buggy(self):
            return self.static(1,2)

    class Slots(dict):
        'doc'
        pass

    class Foo(object):
        'doc'
        __slots__ = Slots()

except NameError:
    pass

