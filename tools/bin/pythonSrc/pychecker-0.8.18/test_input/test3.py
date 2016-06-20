
"Shouldn't be any warnings/errors"

import string

def describeSyntax(syntax):
    return string.join(['<%s>' % x.Description])

from UserDict import UserDict

class jj(UserDict) :
    def __init__(self):
      UserDict.__init__(self)

