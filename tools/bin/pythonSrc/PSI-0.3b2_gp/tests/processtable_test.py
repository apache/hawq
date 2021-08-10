# The MIT License
#
# Copyright (C) 2008-2009 Floris Bruynooghe
#
# Copyright (C) 2008-2009 Abilisoft Ltd.
#
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import os
import unittest

import psi


class ProcessTableTests(unittest.TestCase):
    def setUp(self):
        self.archtype = psi.arch.arch_type()
        self.pt = psi.process.ProcessTable()

    def test_type(self):
        self.assert_(isinstance(self.pt, psi.process.ProcessTable))
        self.assert_(isinstance(self.pt, dict))

    def test_len_nonzero(self):
        self.assert_(len(self.pt) > 0)

    def test_keys(self):
        self.assert_(1 in self.pt)

    def test_vals(self):
        init = self.pt[1]
        self.assert_(isinstance(init, psi.process.Process))

    def test_setitem(self):
        self.assertRaises(TypeError, self.pt.__setitem__, 123, 'dummy')

    def test_delitem(self):
        self.assertRaises(TypeError, self.pt.__delitem__, 1)


class ProcessAttributeTests(unittest.TestCase):
    """Check the bahaviour of some process attributes

    Some process attributes must be present on all processes, these
    tests check for this.
    """
    def setUp(self):
        self.archtype = psi.arch.arch_type()

    def test_name(self):
        for p in psi.process.ProcessTable().values():
            self.assert_(p.name, str(p))

    def test_argc(self):
        for p in psi.process.ProcessTable().values():
            try:
                self.assert_(p.argc >= 0, '%s, argc=%s' % (p, p.argc))
            except psi.AttrInsufficientPrivsError:
                if isinstance(self.archtype, psi.arch.ArchDarwin):
                    self.assert_(p.euid != os.geteuid())
                else:
                    raise
    
    def test_command(self):
        for p in psi.process.ProcessTable().values():
            self.assert_(p.command, str(p))


if __name__ == '__main__':
    unittest.main()
