from pychecker2 import TestSupport
from pychecker2 import VariableChecks

class SelfTestCase(TestSupport.WarningTester):
    def testSelf(self):
        self.warning('class C:\n'
                     '  def f(x): return x\n',
                     2, VariableChecks.SelfCheck.selfName,
                     'f', 'x', "['self', 'this', 's']")

    def testSelfNames(self):
        self.silent('class C:\n'
                    '  def f(self): return self\n')
        self.argv = ["--selfNames=['x']"]
        self.warning('class C:\n'
                     '  def f(self): return self\n',
                     2, VariableChecks.SelfCheck.selfName,
                     'f', 'self', "['x']")

    def testSelfDefault(self):
        w = VariableChecks.SelfCheck.selfDefault
        self.warning('class C:\n'
                     '  def f(s=None): return s\n', 2, w, 'f', 's')
        self.warning('class C:\n'
                     '  def f(s=None, **kw): return s, kw\n', 2, w, 'f', 's')
        self.warning('class C:\n'
                     '  def f(s=None, *args): return s, args\n',
                     2, w, 'f', 's')
        self.warning('class C:\n'
                     '  def f(s=None, *args, **kw): return s, args, kw\n',
                     2, w, 'f', 's')
        
    def testFunctionSelf(self):
        w = VariableChecks.SelfCheck.functionSelf
        self.warning('def f(a, self, b): return a + self + b\n',
                     1, w, 'f', 'self')
        self.argv = ["--selfSuspicious=['a']"]
        self.warning('def f(a, self, b): return a + self + b\n',
                     1, w, 'f', 'a')

    def testMissingSelf(self):
        w = VariableChecks.SelfCheck.missingSelf
        self.warning('class C:\n'
                     '  def f(): pass\n', 2, w, 'f')

