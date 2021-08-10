'doc'

import fnmatch

class X:
    'doc'
    x = filter(lambda f, p = '' : fnmatch.fnmatch(f, p), [])

