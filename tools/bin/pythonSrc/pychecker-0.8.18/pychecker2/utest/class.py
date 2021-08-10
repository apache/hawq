from pychecker2.TestSupport import WarningTester
from pychecker2 import ClassChecks

class ClassTestCase(WarningTester):
    def testClassChecks(self):
        self.silent('class A:\n'
                    '  def f(self):\n'
                    '    return 0\n'
                    'class B(A):\n'
                    '  def g(self):\n'
                    '    return self.f\n')
    def testSignature(self):
        w = ClassChecks.AttributeCheck.signatureChanged
        self.silent('class Base:\n'
                    '  def f(self): pass\n'
                    'class Derived(Base):\n'
                    '  def f(self): pass\n')
        self.warning('class Base:\n'
                     '  def f(self): pass\n'
                     'class Derived(Base):\n'
                     '  def f(self, x): pass\n',
                     4, w, 'f', 'Base')
        self.silent('class Base:\n'
                    '  def f(self, x, y, *args): pass\n'
                    'class Derived(Base):\n'
                    '  def f(self, x, *args): pass\n')
        self.warning('class Base:\n'
                     '  def f(self, x, *args): pass\n'
                     'class Derived(Base):\n'
                     '  def f(self, x, y, *args): pass\n',
                     4, w, 'f', 'Base')
        self.silent('class Base:\n'
                    '  def __private(self, x, y): pass\n'
                    'class Derived(Base):\n'
                    '  def __private(self, x): pass\n')
        self.warning('class Base:\n'
                     '  def __private(self, x, y): pass\n'
                     'class Derived(Base):\n'
                     '  def _Base__private(self, x): pass\n',
                     4, w, '__private', 'Base')

    def testUnknownAttribute(self):
        w = ClassChecks.AttributeCheck.unknownAttribute
        self.warning('class C:\n'
                     '  def f(self): return self.x\n', 2, w, 'C', 'x')
        self.silent('class C:\n'
                    '  def f(self): return self.x\n'
                    '  def __init__(self, v): self.x = v\n')
        self.silent('class D:\n'
                    '  def __init__(self): self.x = 1\n'
                    'class C(D):\n'
                    '  def f(self): return self.x\n')
        self.silent('from pychecker2.utest.data import *\n'
                    'class C(Data):\n'
                    '  def get_value2(self):\n'
                    '    return self.get_value()\n'
                    '\n')
        imp = '\n\nimport'
        frm = '\n\nfrom'
        import_abuse = [
            (imp, 'pychecker2.utest.data', 'pychecker2.utest.data.Data'),
            (imp, 'pychecker2.utest.data as F',             'F.Data'),
            (frm, 'pychecker2.utest import data',           'data.Data'),
            (frm, 'pychecker2.utest import data as F',      'F.Data'),
            (frm, 'pychecker2.utest.data import Data',      'Data'),
            (frm, 'pychecker2.utest.data import Data as F', 'F'),
            (frm, 'pychecker2.utest.data import *',         'Data'),
            ('class A:\n  class B:\n    import pychecker2.utest.data',
             '',
             'A.B.pychecker2.utest.data.Data'),
            ('class A:\n'
             '  class B:\n'
             '    from pychecker2.utest import data as F',
             '',
             'A.B.F.Data')
            ]
        for fmt in import_abuse:
            self.silent('%s %s\n'
                        'class C(%s):\n'
                        '  def f(self): return self.value\n' % fmt)
            self.silent('%s %s\n'
                        'def g(v):\n'
                        '  class C(%s):\n'
                        '    def f(self): return self.value\n'
                        '  return C(v)\n' % fmt)
            self.warning('%s %s\n'
                         'class C(%s):\n'
                         '  def f(self): self.get_value = 1\n' % fmt,
                         5, ClassChecks.AttributeCheck.methodRedefined,
                         'get_value', 'C')

            self.warning('%s %s\n'
                         'def g(v):\n'
                         '  class C(%s):\n'
                         '    def f(self): self.get_value = 1\n'
                         '  return C(v)\n' % fmt,
                         6, ClassChecks.AttributeCheck.methodRedefined,
                         'get_value', 'C')

            self.warning('%s %s\n'
                         'class C(%s):\n'
                         '  def get_value(self, x): pass\n' % fmt,
                         5, ClassChecks.AttributeCheck.signatureChanged,
                         'get_value', 'Data')

    def testSpecial(self):
        for fmt in [('__del__', 'self, args', 1),
                    ('__cmp__', 'self', 2),
                    ]:
            self.warning('class C:\n'
                         '  def %s(%s):\n'
                         '     pass\n' % fmt[0:2],
                         2, ClassChecks.SpecialCheck.specialMethod,
                         fmt[0], fmt[2], fmt[2] > 1 and "s" or "")
        for fmt in [('__del__', 'self'),
                    ('__ge__', 'self, a, b = None, c = None'),
                    ]:
            self.silent('class C:\n'
                         '  def %s(%s):\n'
                         '     pass\n' % fmt)
        self.warning('class C:\n'
                     '  def __not_special__(self):\n'
                     '     pass\n',
                     2, ClassChecks.SpecialCheck.notSpecial,
                     '__not_special__')

    def testUncheckableAttribute(self):
        # inherit from local variable
        self.silent('def f(klass):\n'
                    '  class C(klass):\n'
                    '      def __init__(self, x):\n'
                    '          self.foo = x\n'
                    '      def g(self): return self.foo\n'
                    '  return C(1)\n')
        # inherit from an expression
        self.silent('class A:\n'
                    '  def __add__(self, unused):\n'
                    '     return A\n'
                    'class B(A): pass\n'
                    'class C(A() + B()): pass\n')
        # inherit from something not in a source module
        self.warning('import exceptions\n'
                     'class C(exceptions):\n'
                     '  def f(self):\n'
                     '    return self.value\n',
                     4, ClassChecks.AttributeCheck.unknownAttribute,
                     'C', 'value')
        self.warning('import pychecker2.utest.data\n'
                     'class C(pychecker2.utest.data.exceptions.AssertionError):\n'
                     '  def f(self):\n'
                     '    return self.value\n',
                     4, ClassChecks.AttributeCheck.unknownAttribute,
                     'C', 'value')
        self.warning('from pychecker2.utest.data import Data as F\n'
                     'class C(F.DataError):\n'
                     '  def f(self):\n'
                     '    return self.value\n',
                     4, ClassChecks.AttributeCheck.unknownAttribute,
                     'C', 'value')
        self.silent('def f():\n'
                    '   class Foo(None.__class__): pass\n'
                    '   return Foo()\n')

    def testUnused(self):
        self.warning('class C:\n'
                     '  def __init__(self):\n'
                     '    self.__x = 1\n',
                     3, ClassChecks.AttributeCheck.unusedAttribute, '__x', 'C')
        self.warning('class C:\n'
                     '  def __init__(self):\n'
                     '    class D:\n'
                     '       def __init__(self): self.__x = 1\n'
                     '       def f(self):\n'
                     '          return self.__x\n'
                     '    self.__x = D()\n',
                     7, ClassChecks.AttributeCheck.unusedAttribute, '__x', 'C')
        self.silent('class C:\n'
                     '  def __init__(self):\n'
                     '    self.x = 1\n')

    def testInit(self):
        w = ClassChecks.InitCheck.initReturnsValue
        self.warning('class C:\n'
                     '  def __init__(self):\n'
                     '    return 1\n', 3, w)
        self.warning('class C:\n'
                     '  def __init__(self):\n'
                     '    def f(x):\n'
                     '        return f(x - 1)\n'
                     '    return f(1)\n', 5, w)

        w = ClassChecks.AttributeCheck.attributeInitialized
        self.silent('class C:\n'
                    '  def f(self):\n'
                    '    self.value = 1\n'
                    '  def __init__(self):\n'
                    '    self.value = None\n')
        self.warning('class C:\n'
                     '  def __init__(self): pass\n'
                     '  def f(self):\n'
                     '    self.value = 1',
                     4, w, 'value')

    def testRepr(self):
        w = ClassChecks.ReprCheck.backquoteSelf
        self.silent('class C:\n'
                    '  def __str__(self):\n'
                    '    return "C" + `self`\n')
        self.warning('class C:\n'
                    '  def __repr__(self):\n'
                    '    return "C" + `self`\n', 3, w)

    def testMethodRedefined(self):
        w = ClassChecks.AttributeCheck.methodRedefined

        self.warning('class C:\n'
                     '  def f(self):\n'
                     '     self.f = lambda x: x\n',
                     3, w, 'f', 'C')
        self.warning('class B:\n'
                     '  def g(self): pass\n'
                     'class C(B):\n'
                     '  def f(self):\n'
                     '     self.g = lambda x: x\n',
                     5, w, 'g', 'C')
        self.warning('from pychecker2.utest.data import Data\n'
                     'class C(Data):\n'
                     '  def f(self):\n'
                     '    self.get_value = 1\n',
                     4, w, 'get_value','C')


