from pychecker2 import TestSupport
from pychecker2 import VariableChecks

class ShadowTestCase(TestSupport.WarningTester):
    def testShadow(self):
        "Test variable shadowing"
        self.warning('a = 1\n'
                     'def f(x):\n'
                     '  a = x\n'
                     '  return x + a\n',
                     3, VariableChecks.ShadowCheck.shadowIdentifier,
                     'a', '<ModuleScope: global>')
        self.warning('file = None\n',
                     1, VariableChecks.ShadowCheck.shadowBuiltins, 'file')

        w = VariableChecks.ShadowCheck.defineNone
        self.silent('none, x = 1, 2')
        self.warning('None = None', 1, w)
        self.warning('def None(x, y): pass', 1, w)
        self.warning('None, x = 1, 2', 1, w)
        self.warning('class None: pass', 1, w)
