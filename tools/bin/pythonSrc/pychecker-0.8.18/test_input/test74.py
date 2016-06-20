'test __getattr[ibute]__ returning None'

class A:
    'no warning'
    def __getattr__(self, attr):
        return attr

class B:
    'no warning'
    def __getattribute__(self, attr):
        return attr

class C:
    'warning'
    def __getattr__(self, attr):
        pass

class D:
    'warning'
    def __getattribute__(self, attr):
        pass

class E:
    'warning'
    def __getattr__(self, attr):
        if attr == 'n':
            return attr
        if attr != 'j':
            raise AttributeError

class F:
    'no warning'
    def __getattr__(self, attr):
        if attr == 'n':
            return attr
        raise AttributeError

class G:
    'should not gen a warning'
    def __getattr__(self, name):
        return getattr(self, 'a')[name]
