from pychecker2 import TestSupport
from pychecker2 import ParseChecks
from pychecker2 import VariableChecks
from pychecker2.File import File

class UnknownTestCase(TestSupport.WarningTester):
    def testParseError(self):
        self.warning('===\n', 1, ParseChecks.ParseCheck.syntaxErrors,
                     'could not parse string')
        f = File('no-such-file')
        self.checklist.check_file(f)
        self.warning_file(f, 0, ParseChecks.ParseCheck.syntaxErrors,
                          "No such file or directory")

        self.warning('if __name__=="__main__":\n'
                     '  _x = 1\n',
                     2, VariableChecks.UnusedCheck.unused, '_x')
        self.argv = ['--no-main']
        self.silent('NoMoreGood = 1\n'
                    'assert NoMoreGood\n')
        self.silent('if __name__=="__main__":\n'
                     '  _x = 1\n')
        self.silent('if "__main__"==__name__:\n'
                     '  _x = 1\n')
