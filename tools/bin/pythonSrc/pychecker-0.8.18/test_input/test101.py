# -*- Mode: Python -*-
# vi:si:et:sw=4:sts=4:ts=4

# see https://sourceforge.net/tracker/index.php?func=detail&aid=1563495&group_id=24686&atid=382217

'd'

__pychecker__ = 'blacklist=wrongname'

import os
import imp
import tempfile

code = """
class C:
    pass

c = C()
"""

# create a temporary file for the module
(fd, path) = tempfile.mkstemp()
os.write(fd, code)
os.close(fd)

temp = imp.load_source('temp', path)

# change class's module to trigger the error
temp.C.__module__ = 'wrongname'

# clean up
os.unlink(path)

