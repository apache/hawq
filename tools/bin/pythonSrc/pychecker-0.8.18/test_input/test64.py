'''spurious warnings reported by Andrew Dalke
'''

import sys, string, re
import array

class _Anything: pass
_anything = _Anything()

def outer():
    def x():
        return _anything

    def y():
        return dir(string.translate)

    x()
