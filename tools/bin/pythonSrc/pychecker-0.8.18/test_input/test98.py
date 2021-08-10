# -*- encoding: latin-1 -*-

__doc__ = 'return value from init reports proper file names'

from import98 import BaseGood, BaseBad

class GoodGood(BaseGood):
    'doc'
    def __init__(self):
        BaseGood.__init__(self)

class GoodBad(BaseGood):
    'doc'
    def __init__(self):
        return BaseGood.__init__(self)

class GoodVoid(BaseGood):
    'doc'
    pass

class BadGood(BaseBad):
    'doc'
    def __init__(self):
        BaseBad.__init__(self)

class BadBad(BaseBad):
    'doc'
    def __init__(self):
        return BaseBad.__init__(self)

class BadVoid(BaseBad):
    'doc'
    pass
