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


class ArchTests(unittest.TestCase):
    def setUp(self):
        self.arch = psi.arch.arch_type()

    def test_type(self):
        self.failUnless(isinstance(self.arch, psi.arch.ArchBase))

    def test_singleton(self):
        base = psi.arch.ArchBase()
        self.assertEqual(id(self.arch), id(base))

    def test_sysname(self):
        self.assertEqual(self.arch.sysname, os.uname()[0])

    def test_nodename(self):
        self.assertEqual(self.arch.nodename, os.uname()[1])

    def test_release(self):
        self.assertEqual(self.arch.release, os.uname()[2])

    def test_release_info(self):
        rel = os.uname()[2].split('-')[0]
        rel_info = tuple([int(i) for i in rel.split('.')])
        self.assertEqual(self.arch.release_info, rel_info)

    def test_version(self):
        self.assertEqual(self.arch.version, os.uname()[3])

    def test_machine(self):
        self.assertEqual(self.arch.machine, os.uname()[4])


if __name__ == '__main__':
    unittest.main()
