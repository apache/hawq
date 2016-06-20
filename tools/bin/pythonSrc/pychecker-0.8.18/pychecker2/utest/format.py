from pychecker2 import TestSupport
from pychecker2 import FormatStringChecks

class FormatTestCase(TestSupport.WarningTester):

    def testGoodFormats(self):
        self.silent('def f(x):\n'
                    '    return "%s" % x\n')
        self.silent('def f(x):\n'
                    "    return ('%s' + '%s') % (x, x)\n")
        self.silent("def f(x):\n"
                    "   return (('%s' + '%s') * 8) % ((x,) * 16)\n")
        self.silent("def f(x):\n"
                    "   y = 2\n"
                    "   return '%(x)f %(y)s' % locals()\n")
        self.silent("y = 1\n"
                    "def f():\n"
                    "   return '%(y)s' % globals()\n")
        self.silent("def f():\n"
                     "  return '%*.s %*.*s %*f' % locals()\n")
        self.silent("def f():\n"
                    "   return '%s %%' % ('',)\n")
        self.silent("def f(t):\n"
                    "    return '%s %f' % t\n")
        self.silent("def f(t):\n"
                    "    return ('%s %f' + t) % (1, 2)\n")
        self.silent("def f(t):\n"
                    "    return '%s' % `t`\n")
        self.silent("def f(t):\n"
                    "    return '%s' * ((7 - 1) / 2) % (t,t,t)\n")

    def testBadFormats(self):
        w = FormatStringChecks.FormatStringCheck.badFormat
        self.warning("def f():\n"
                     "    return '%' % locals()\n", 2, w, 0, '%')
        self.warning("def f():\n"
                     "  return '%z a kookie format, yah' % locals()\n",
                     2, w, 0, '%z a kooki...')
        self.warning("def f():\n"
                     "    return '%(foo)*.*s' % {'foo': 'bar'}\n",
                     2, w, 0, '%(foo)*.*s')

    def testMixed(self):
        w = FormatStringChecks.FormatStringCheck.mixedFormat
        self.warning("def f():\n"
                     "  return '%(mi)x %up' % locals()\n", 2, w, '(mi)')
        self.warning("def f():\n"
                     "    return '%up %(mi)x' % (1, 2)\n", 2, w, '(mi)')

    def testFormatCount(self):
        w = FormatStringChecks.FormatStringCheck.formatCount
        self.warning("def f():\n"
                     "    return '%s %d %f' % ('', 2)\n",
                     2, w, 2, 3)

    def testUselessModifier(self):
        w = FormatStringChecks.FormatStringCheck.uselessModifier
        self.warning("def f(t):\n"
                     "    return '%s %lf' % (t, t)\n",
                     2, w, 'l')

    def testFormatConstants(self):
        w = FormatStringChecks.FormatStringCheck.badConstant
        self.warning("def f():\n"
                    "    return ('%s' * 6) % ((1, 2) + 3 * 7)\n",
                     2, w, 'can only concatenate tuple (not "int") to tuple')
        self.warning("def f():\n"
                    "    return ('%s' + 6) % ((1, 2) * 3)\n",
                     2, w, "cannot concatenate 'str' and 'int' objects")

    def testUnknownName(self):
        w = FormatStringChecks.FormatStringCheck.unknownFormatName
        self.warning("def f():\n"
                     "   return '%(unknown)s' % globals()\n",
                     2, w, "unknown", "globals")
        self.warning("def f():\n"
                     "   return '%(unknown)s' % locals()\n",
                     2, w, "unknown", "locals")
