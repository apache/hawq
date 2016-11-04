# The MIT License
#
# Copyright (C) 2009 Erick Tryzelaar
#
# Copyright (C) 2009 Floris Bruynooghe
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


import sys
import time
import unittest

import psi

try:
    import datetime
except ImportError:
    HAVE_DATETIME = False
else:
    HAVE_DATETIME = True


class TimeSpecTests(unittest.TestCase):
    def setUp(self):
        self.tv = psi.TimeSpec(5, 10)

    def test_type(self):
        self.assert_(isinstance(self.tv, psi.TimeSpec))

    def test_init(self):
        tv = psi.TimeSpec(5, 10)
        self.assertEqual(tv.tv_sec, 5)
        self.assertEqual(tv.tv_nsec, 10)
        tv = psi.TimeSpec(tv_sec=6, tv_nsec=11)
        self.assertEqual(tv.tv_sec, 6)
        self.assertEqual(tv.tv_nsec, 11)

    def test_init_normalise(self):
        t = psi.TimeSpec(0, 1000000001)
        self.assertEqual(t.tv_sec, 1)
        self.assertEqual(t.tv_nsec, 1)
        t = psi.TimeSpec(-1, -1000000001)
        self.assertEqual(t.tv_sec, -1)
        self.assertEqual(t.tv_nsec, -999999999)
        t = psi.TimeSpec(0, -2000000000)
        self.assertEqual(t.tv_sec, -2)
        self.assertEqual(t.tv_nsec, 0)
        t = psi.TimeSpec(-1, 500000000)
        self.assertEqual(t.tv_sec, 0)
        self.assertEqual(t.tv_nsec, -500000000)
        t = psi.TimeSpec(1, -500000000)
        self.assertEqual(t.tv_sec, 0)
        self.assertEqual(t.tv_nsec, 500000000)

    def test_tuple(self):
        self.assertEqual(self.tv[0], 5)
        self.assertEqual(self.tv[1], 10)
        self.assertRaises(IndexError, self.tv.__getitem__, -1)
        self.assertRaises(IndexError, self.tv.__getitem__, 2)

    def test_hash(self):
        self.assertEqual(hash(self.tv), hash(psi.TimeSpec(5, 10)))

    def test_eq(self):
        self.assertEqual(self.tv, psi.TimeSpec(5, 10))

    def test_ne(self):
        self.assertNotEqual(self.tv, psi.TimeSpec(5, 11))
        self.assertNotEqual(self.tv, psi.TimeSpec(4, 10))
        self.assertNotEqual(self.tv, psi.TimeSpec(4, 11))

    def test_lt(self):
        self.assert_(self.tv < psi.TimeSpec(5, 11))
        self.assert_(self.tv < psi.TimeSpec(6, 10))
        self.assert_(not self.tv < psi.TimeSpec(5, 9))
        self.assert_(not self.tv < psi.TimeSpec(5, 10))

    def test_lt_distorted(self):
        self.assert_(self.tv < psi.TimeSpec(4, 1000000015))

    def test_le(self):
        self.assert_(self.tv <= psi.TimeSpec(5, 11))
        self.assert_(self.tv <= psi.TimeSpec(5, 10))
        self.assert_(not self.tv <= psi.TimeSpec(5, 9))

    def test_gt(self):
        self.assert_(self.tv > psi.TimeSpec(5, 9))
        self.assert_(not self.tv > psi.TimeSpec(5, 10))

    def test_gt_distorted(self):
        self.assert_(psi.TimeSpec(4, 1000000015) > self.tv)

    def test_ge(self):
        self.assert_(self.tv >= psi.TimeSpec(5, 10))
        self.assert_(self.tv >= psi.TimeSpec(5, 9))
        self.assert_(not self.tv >= psi.TimeSpec(5, 11))

    def test_compare_tuple(self):
        self.assert_(self.tv > (5, 9))
        self.assert_(self.tv >= (5, 9))
        self.assert_(self.tv == (5, 10))
        self.assert_((5, 10) == self.tv)
        self.assert_(self.tv <= (5, 11))
        self.assert_(self.tv < (5, 11))

    def test_compare_int(self):
        self.assert_(self.tv > 5)
        self.assert_(self.tv != 5)
        self.assert_(self.tv < 6)

    def test_compare_float(self):
        self.assert_(self.tv > 5.0)
        self.assert_(self.tv != 5.0)
        self.assert_(self.tv < 5.1)

    if sys.version_info[0] < 3:
        def test_compare_tuple_long(self):
            self.assert_(self.tv == (long(5), long(10)))

    def test_add(self):
        self.assertEqual(self.tv + psi.TimeSpec(5, 10), psi.TimeSpec(10, 20))

    def test_add_rollover(self):
        sum = self.tv + psi.TimeSpec(0, 999999995)
        self.assertEqual(sum, psi.TimeSpec(6, 5))

    def test_add_tuple(self):
        self.assertEqual(self.tv + (5, 10), psi.TimeSpec(10, 20))

    def test_add_tuple_bad(self):
        self.assertEqual(self.tv.__add__((5, 10, 0)), NotImplemented)

    def test_add_int(self):
        self.assertEqual(self.tv + 5, psi.TimeSpec(10, 10))

    def test_add_float(self):
        self.assertEqual(self.tv + 5.5, psi.TimeSpec(10, 500000010))

    def test_sub(self):
        self.assertEqual(self.tv - psi.TimeSpec(1, 5), psi.TimeSpec(4, 5))

    def test_sub_rollover(self):
        self.assertEqual(self.tv - psi.TimeSpec(0, 15),
                         psi.TimeSpec(4, 999999995))

    def test_sub_tuple(self):
        self.assertEqual(self.tv - (1, 5), psi.TimeSpec(4, 5))

    def test_sub_tuple_bad(self):
        self.assertEqual(self.tv.__sub__((1, 5, 0)), NotImplemented)

    def test_sub_int(self):
        self.assertEqual(self.tv - 1, psi.TimeSpec(4, 10))

    def test_sub_float(self):
        self.assertEqual(self.tv - 1.5, psi.TimeSpec(3, 500000010))

    def test_bool(self):
        self.assertEqual(bool(self.tv), True)
        self.assertEqual(bool(psi.TimeSpec(0, 0)), False)
        self.assertEqual(bool(psi.TimeSpec(-1, -1)), True)


class TimeSpecMethodsTests(unittest.TestCase):
    def setUp(self):
        self.tv = psi.TimeSpec(5, 500000000)

    def test_timestamp(self):
        self.assertEqual(self.tv.timestamp(), 5.5)

    def test_float(self):
        self.assertEqual(self.tv.float(), 5.5)

    def test_mktime(self):
        if time.daylight:
            t = 5.5 + time.timezone + time.altzone
        else:
            t = 5.5 + time.timezone
        self.assertEqual(self.tv.mktime(), t)

    def test_timetuple(self):
        self.assertEqual(self.tv.timetuple(), time.gmtime(5.5))

    def test_gmtime(self):
        self.assertEqual(self.tv.gmtime(), time.gmtime(5.5))

    def test_localtime(self):
        self.assertEqual(self.tv.localtime(), time.localtime(5.5))

    def test_utcdatetime(self):
        if HAVE_DATETIME:
            self.assertEqual(self.tv.utcdatetime(),
                             datetime.datetime.utcfromtimestamp(5.5))
        else:
            self.assert_(not hasattr(self.tv, 'utcdatetime'))

    def test_datetime(self):
        if HAVE_DATETIME:
            self.assertEqual(self.tv.datetime(),
                             datetime.datetime.fromtimestamp(5.5))
        else:
            self.assert_(not hasattr(self.tv, 'datetime'))

    def test_timedelta(self):
        if HAVE_DATETIME:
            self.assertEqual(self.tv.timedelta(),
                             datetime.timedelta(seconds=5, microseconds=500000))
        else:
            self.assert_(not hasattr(self.tv, 'timedelta'))


if __name__ == '__main__':
    unittest.main()
