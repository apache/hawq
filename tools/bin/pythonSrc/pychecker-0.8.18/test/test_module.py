# -*- Mode: Python -*-
# vi:si:et:sw=4:sts=4:ts=4

'''
Tests related to modules.
'''

import unittest
import common

class SameModuleNameTestCase(common.TestCase):
    '''
    Test that modules with the same name do not shadow eachother.
    '''
    def test_getmodule(self):
        self.checkMultiple('test_getmodule', [
            'getmodule/A/C.py',
            'getmodule/B/C.py',
            ])
    
if __name__ == '__main__':
    unittest.main()
