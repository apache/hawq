# The MIT License
#
# Copyright (C) 2009 Erick Tryzelaar
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

import apphelper
import psi.arch

import psi.mount


class MountAttrTests(unittest.TestCase):
    def setUp(self):
        self.mounts = psi.mount.mounts()
        for mount in psi.mount.mounts():
            if mount.mountpoint == '/':
                break
        self.m = mount
        if isinstance(psi.arch.arch_type(), psi.arch.ArchLinux):
            fd = open('/etc/mtab')
            mtab = fd.readlines()
            fd.close()
            for line in mtab:
                if line.split()[1] == '/':
                    break
            mount = line.split()
            self.device = mount[0]
            self.mountpoint = mount[1]
            self.fstype = mount[2]
            self.options = mount[3]
        elif isinstance(psi.arch.arch_type(), psi.arch.ArchSunOS):
            mounts = apphelper.run(['/usr/sbin/mount', '-p']).split('\n')
            for line in mounts:
                if line.split()[2] == '/':
                    break
            mount = line.split()
            self.device = mount[0]
            self.mountpoint = mount[2]
            self.fstype = mount[3]
            if len(mount) >= 7:
                self.options = mount[6]
            else:
                self.options = ''
            # Special case to find dev=XXXXX option
            if psi.arch.arch_type().release_info > (5, 8):
                mounts = apphelper.run(['/usr/sbin/mount', '-v']).split('\n')
                for line in mounts:
                    if line.split()[2] == '/':
                        break
                opts = line.split()[5]
                opts = opts.split('/')
                for o in opts:
                    if o[:4] == 'dev=':
                        break
                self.options += ',' + o
                self.options = self.options.strip(',')
        elif isinstance(psi.arch.arch_type(), psi.arch.ArchAIX):
            mounts = apphelper.run(['/usr/sbin/mount']).split('\n')[2:]
            for line in mounts:
                if line.split()[1] == '/':
                    break
            mount = line.split()
            self.device = mount[0]
            self.mountpoint = mount[1]
            self.fstype = mount[2]
            self.options = mount[-1]
        elif isinstance(psi.arch.arch_type(), psi.arch.ArchDarwin):
            mounts = apphelper.run(['/sbin/mount']).split('\n')
            for line in mounts:
                if line.split()[2] == '/':
                    break
            mount = line.split()
            self.device = mount[0]
            self.mountpoint = mount[2]
            self.fstype = mount[3][1:-1]
            self.options = ','.join(line[line.find('('):line.find(')')].split(', ')[1:])

    def test_enumerate(self):
        self.assert_(hasattr(self.mounts, '__iter__'))

    def test_len(self):
        self.assert_(len(list(self.mounts)) > 0)

    def test_type(self):
        for m in self.mounts:
            self.assert_(isinstance(m, psi.mount.MountBase))

    def test_remote(self):
        local = len(list(psi.mount.mounts()))
        remote = len(list(psi.mount.mounts(True)))
        self.assert_(remote >= local, '%d >= %d' % (remote, local))

    def test_device(self):
        self.assertEqual(self.m.device, self.device)

    def test_fstype(self):
        self.assertEqual(self.m.fstype, self.fstype)

    def test_options(self):
        self.assertEqual(self.m.options, self.options)

    def test_mountpoint(self):
        self.assertEqual(self.m.mountpoint, self.mountpoint)

    def test_total(self):
        self.assert_(self.m.total > 0)

    def test_free(self):
        self.assert_(self.m.free > 0)
        self.assert_(self.m.total > self.m.free)

    def test_available(self):
        self.assert_(self.m.available > 0)
        self.assert_(self.m.free >= self.m.available,
                     '%d > %d' % (self.m.free, self.m.available))

    def test_inodes(self):
        self.assert_(self.m.inodes > 0)

    def test_free_inodes(self):
        self.assert_(self.m.free_inodes > 0)
        self.assert_(self.m.inodes > self.m.free_inodes)

    def test_available_inodes(self):
        self.assert_(self.m.available_inodes > 0)
        self.assert_(self.m.free_inodes >= self.m.available_inodes,
                     '%d > %d' % (self.m.free_inodes, self.m.available_inodes))


class MountMethodsTests(unittest.TestCase):
    def setUp(self):
        for mount in psi.mount.mounts():
            break
        self.m = mount

    def test_refresh(self):
        mp = self.m.mountpoint
        self.m.refresh()
        self.assertEqual(mp, self.m.mountpoint)


if __name__ == '__main__':
    unittest.main()
