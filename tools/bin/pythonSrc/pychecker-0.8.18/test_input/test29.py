'doc'

import fnmatch

class X:
    'doc'
    def x(self):
        x = filter(lambda f, p = '' : fnmatch.fnmatch(f, p), [])
        print x

def x(cpu):
    p = len(filter(lambda x: x.find('processor') > -1, cpu.items))
    print p

