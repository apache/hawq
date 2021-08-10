from pychecker2.TestSupport import WarningTester
from pychecker2 import ScopeChecks

class RedefinedTestCase(WarningTester):
    def testScopes(self):
        w = ScopeChecks.RedefineCheck.redefinedScope
        self.warning('def f(): pass\n'
                     'def f(): pass\n',
                     1, w, 'f', 2)
        self.warning('class C:\n'
                     '  def g(self): pass\n'
                     '  def g(self): pass\n',
                     2, w, 'g', 3)
        self.silent('def s(): pass\n'
                    'def f(): pass\n')
        self.silent('import sys\n'
                    'if sys.argv:\n'
                    '   def f(): return 1\n'
                    'else:\n'
                    '   def f(): return 0\n')
        self.warning('import sys\n'
                     'if sys.argv:\n'
                     '   def f(): return 1\n'
                     '   def f(): return 0\n',
                     3, w, 'f', 4)
        self.warning('try:\n'
                     '   def f(): return 1\n'
                     'except Exception:\n'
                     '   pass\n'
                     'else:\n'
                     '   def f(): return 0\n',
                     2, w, 'f', 6)
        self.warning('try:\n'
                     '   def f(): return 1\n'
                     '   def f(): return 0\n'
                     'except Exception:\n'
                     '   pass\n'
                     'else:\n'
                     '   pass\n',
                     2, w, 'f', 3)

        self.silent('try:\n'
                    '   def f(): return 1\n'
                    'except Exception:\n'
                    '   def f(): return 0\n'
                    'else:\n'
                    '   pass\n')
