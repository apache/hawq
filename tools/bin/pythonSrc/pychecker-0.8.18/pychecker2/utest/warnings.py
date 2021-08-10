from pychecker2 import TestSupport
from pychecker2 import OpChecks
from pychecker2 import main

from StringIO import StringIO

class OpTests(TestSupport.WarningTester):

    def _get_warnings(self):
        out = StringIO()
        f = self.check_file('def f(x):\n'
                            '   return +x\n')
        assert len(f.warnings) == 1
        line, warning, args = f.warnings[0]
        main.print_warnings(f, out)
        return out.getvalue()
    
    def testWarningSuppression(self):
        # check cmd-line warning suppression
        assert self._get_warnings() != ''
        self.argv = ['--no-operatorPlus']
        assert self._get_warnings() == ''

