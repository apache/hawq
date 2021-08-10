from pychecker2 import TestSupport
from pychecker2 import VariableChecks

class UnknownTestCase(TestSupport.WarningTester):
    def testUnknown(self):
        self.warning('def f(): print a\n',
                     1, VariableChecks.UnknownCheck.unknown, 'a')
        self.silent('def f():\n'
                    '   a = 1\n'
                    '   def g():\n'
                    '      print a\n'
                    '   print g()\n')
        self.warning('def f():\n'
                     '  def g():\n'
                     '    print a\n'
                     '  print g()\n',
                     3, VariableChecks.UnknownCheck.unknown, 'a')
        self.silent('from sys import *\n'
                    'def f():\n'
                    '    print argv\n')
        self.silent('import sys\n'
                    'def f():\n'
                    '  def g():\n'
                    '    print g, sys\n')
        self.silent('def f():\n'
                    '  for a, b in [(1,2)]:\n'
                    '    print a, b\n')
