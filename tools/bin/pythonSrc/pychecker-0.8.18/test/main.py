# -*- Mode: Python -*-
# vi:si:et:sw=4:sts=4:ts=4

# import all TestCase from all test modules
# FIXME: we could autodiscovery all test_ modules
# note: this means all TestCase classes need a different name, might
# be a problem if one shadows another accidentally ?

from test_stdlib import *
from test_module import *

if __name__ == '__main__':
    unittest.main()
