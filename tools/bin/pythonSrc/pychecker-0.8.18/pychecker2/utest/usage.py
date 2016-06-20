from pychecker2.TestSupport import WarningTester
from StringIO import StringIO

class UsageTestCase(WarningTester):
    def testUsage(self):
        self.options.usage(['-?'], StringIO())

