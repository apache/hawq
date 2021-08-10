# -*- Mode: Python -*-
# vi:si:et:sw=4:sts=4:ts=4

'''
Tests related to the -q/--stdlib option.
'''

import unittest
import common

class ZopeTestCase(common.TestCase):
    '''
    test that -q properly ignores errors for modules in the arch-specific
    directory.

    See http://sourceforge.net/tracker/index.php?func=detail&aid=1564614&group_id=24686&atid=382217
    '''
    def test_zope_interface(self):
        if not common.canImport('zope.interface'):
            self.skip = True # FIXME: interpret this

        self.check('test_zope_interface', '-q')
    
if __name__ == '__main__':
    unittest.main()
