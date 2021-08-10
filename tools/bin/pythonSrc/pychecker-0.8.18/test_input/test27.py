'doc'

import fnmatch

x = []
bfp = ''
x = filter(lambda f, p = bfp : fnmatch.fnmatch(f, p), x)

def y():
    print x

def d(lines):
    import string
    lines = map(lambda s: string.strip(s), lines)

