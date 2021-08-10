from pychecker2.TestSupport import WarningTester
from pychecker2 import ReturnChecks

class ReturnTestCase(WarningTester):
    def testReturnChecks(self):
        w = ReturnChecks.MixedReturnCheck.mixedReturns
        self.silent('def f(): return\n')
        self.silent('def f(): return 1\n')
        self.silent('def f(x):\n'
                     '  if x:\n'
                     '    return 0\n'
                     '  return 1\n')
        self.warning('def f(x):\n'
                     '  if x:\n'
                     '    return\n'
                     '  return 1\n', 3, w, 'f')
        self.silent('def f(x):\n'
                    '  def g():\n'
                    '    return\n'
                    '  return x + g()\n')

