from pychecker2.TestSupport import WarningTester
from pychecker2 import ConditionalChecks

class ConditionalTestCase(WarningTester):
    def testConstants(self):
        w = ConditionalChecks.ConstantCheck.constantInConditional
        
        self.silent('a, b = 1, 2\n'
                    'if a < b:\n'
                    '   print a\n')
        self.warning('a = 1\n'
                     'if a or 1:\n'
                     '  print a\n', 2, w, '1')
        self.warning('a = 1\n'
                     'if not 1:\n'
                     '  print a\n', 2, w, '1')
        self.silent('a, b = 1, 2\n'
                    'if a < 1 + 7:\n'
                    '   print a\n')
        self.warning('result = [x for x in [1, 2, 3] if x or 1]\n',
                     1, w, '1')
        self.warning('result = [x for x in [1, 2, 3] if x or None]\n',
                     1, w, 'None')
        self.warning('x = 1\n'
                     'while x or None:\n'
                     '   x = x + 1\n',
                     2, w, 'None')
