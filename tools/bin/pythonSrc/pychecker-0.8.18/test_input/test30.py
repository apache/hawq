'doc'

import fnmatch

def x():
    x = filter(lambda f, p = '' : fnmatch.fnmatch(f, p), [])
    print x

