from pychecker2.TestSupport import WarningTester
from pychecker2 import ReachableChecks

class ReachableTestCase(WarningTester):
    def testUnreachable(self):
        w = ReachableChecks.ReachableCheck.unreachable
        self.warning('v1, v2 = 0, 0\n'
                     'def f():\n'
                     '  if v1:\n'
                     '     return v2\n'
                     '  elif v2:\n'
                     '    return 1\n'
                     '  else:\n'
                     '    return 0\n'
                     '  return 1\n', 9, w)
        self.warning('v1, v2 = 0, 0\n'
                     'def f():\n'
                     '  if v1:\n'
                     '    raise AssertionError("assertion")\n'
                     '  elif v2:\n'
                     '    assert 0, "Another error"\n'
                     '  else:\n'
                     '    return 0\n'
                     '  return f()\n', 9, w)
        self.silent('def f(x):\n'
                    '   while x:\n'
                    '      pass\n')
        self.silent('def f(x):\n'
                    '   for i in x:\n'
                    '      assert i\n')
        self.silent('def f(x):\n'
                    '  if x:\n'
                    '    return 0\n'
                    '  return 0\n')
        self.warning('def f(x):\n'
                     '  try:\n'
                     '    x = 1 / x\n'
                     '    return x\n'
                     '  except Exception:\n'
                     '    return 0\n'
                     '  else:\n'
                     '    x = 7\n'
                     '  return x\n', 8, w)

        self.silent('def f(v1, v2):\n'
                    '  if v1:\n'
                    '    return v2\n'
                    '  elif v2:\n'
                    '    v1 = v2\n'
                    '  else:\n'
                    '    assert 0, "Another error"\n'
                    '  return f()\n')

        self.silent('def f(v1, v2):\n'
                    '  if v1:\n'
                    '    return v2\n'
                    '  elif v2:\n'
                    '    v1 = v2\n'
                    '  else:\n'
                    '    assert 0, "Another error"\n'
                    '  return 0\n')

        self.silent('def f(x):\n'
                    '  try:\n'
                    '    return 1.0 / x\n'
                    '  except ZeroDivisionError:\n'
                    '    raise\n')
    
    def testImplicitReturn(self):
        w = ReachableChecks.ReachableCheck.implicitReturn
        self.warning('def f(v1, v2):\n'
                     '  if v1:\n'
                     '    return v2\n', 2, w, 'f')
