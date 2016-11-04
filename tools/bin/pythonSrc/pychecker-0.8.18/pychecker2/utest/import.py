from pychecker2.TestSupport import WarningTester
from pychecker2 import ImportChecks

class ImportTestCase(WarningTester):
    def testImportChecks(self):
        self.silent('import sys; print sys.argv')
        self.silent('import pychecker2; print pychecker2')
        self.silent('import pychecker2.utest; print pychecker2.utest')

    def testImportChecks(self):
        self.warning('import sys\n'
                     'print sys.argv\n'
                     'import sys\n',
                     3, ImportChecks.ImportCheck.duplicateImport,
                     'sys', ' in current scope')
        self.warning('from sys import *\n'
                     'def f():\n'
                     '  def g():\n'
                     '    from sys import argv\n'
                     '    return argv\n'
                     '  return g() + g()\n'
                     'print argv\n',
                     4, ImportChecks.ImportCheck.duplicateImport, 'argv',
                     ' of import in parent scope <ModuleScope: global>')
        self.warning('import no_such_module\n',
                     1, ImportChecks.ImportCheck.importError, 'no_such_module',
                     'No module named no_such_module')
        self.warning('from pychecker2.utest.data import *\n'
                     'import exceptions\n'
                     'print exceptions\n',
                     2, ImportChecks.ImportCheck.shadowImport,
                     'exceptions', 'pychecker2.utest.data', 1)
                     
