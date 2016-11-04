# -*- Mode: Python -*-
# vi:si:et:sw=4:sts=4:ts=4

import os
import commands
import unittest

def canImport(module):
    '''
    Check if we can import the given module name.

    @type module: str

    @rtype: bool
    '''
    try:
        __import__(module)
        return True
    except ImportError:
        return False

# lifted from flumotion with permission
def _diff(old, new, desc):
    import difflib
    lines = difflib.unified_diff(old, new)
    lines = list(lines)
    if not lines:
        return
    output = ''
    for line in lines:
        output += '%s: %s\n' % (desc, line[:-1])

    raise AssertionError(
        ("\nError while comparing strings:\n"
         "%s") % (output,))

def diffStrings(orig, new, desc='input'):
    def tolines(s):
        return [line + '\n' for line in s.split('\n')]

    return _diff(tolines(orig), tolines(new), desc=desc)


# our base class for all tests
class TestCase(unittest.TestCase):
    def check(self, testname, args=''):
        """
        Run pychecker on the given test, located in input/
        Will compare to output of the same name in expected/
        """
        return self.checkMultiple(testname, [testname + '.py'], args)

    def checkMultiple(self, testname, checkables, args=''):
        """
        Run pychecker on the given test, located in input/
        Will compare to output of the same name in expected/

        @type checkables: list of str
        """
        testdir = os.path.dirname(__file__)
        # make this relative to where we are, so paths shown are relative too
        if testdir.startswith(os.getcwd()):
            testdir = testdir[len(os.getcwd()) + 1:]

        pycheckerpy = os.path.join(os.path.dirname(testdir),
            'pychecker', 'checker.py')
        testfiles = [os.path.join(testdir, 'input', c) for c in checkables]

        cmd = "python -tt %s " \
            "--limit 0 --no-argsused " \
            "%s %s" % (pycheckerpy, args, " ".join(testfiles))
        # getoutput output never ends on a newline the way
        # pychecker ... > expected/... would
        output = commands.getoutput(cmd) + '\n'
        
        # here we can select a different file based on os/python version/arch
        expectedfile = os.path.join(testdir, 'expected', testname)

        # FIXME: make generating an option
        # for now, do it every time we don't have the expected output
        # to help us
        if not os.path.exists(expectedfile):
            open(expectedfile, 'w').write(output)

        expected = open(expectedfile).read()

        diffStrings(output, expected, desc=expectedfile)

