
__pychecker__ = '--no-namedargs --no-import --no-var --no-privatevar --no-moduledoc --funcdoc'

import re

_NOT_USED1 = None
NOT_USED2 = None

def x(a, b) : pass

def y():
  x(b=1, a=2)

