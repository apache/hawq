'doc'

class X:
  'doc'
  def __init__(self): pass

class Y(X):
  'doc'
  def __init__(self, s):
    X.__init__(s)

class Z(X):
  'doc'
  def __init__(self, s):
    X.__init__()

