
"time module not used if write32u() line in X.uuu() is commented out"

import time

def write32u(a, b): pass

class X:
  def __init__(self):
    self.fileobj = 0
  def uuu(self): 
    #write32u(self.fileobj, long(time.time()))
    return None

class Y(X):
  def __init__(self) :
    "ERROR: X.__init__() isn't called"
    # X.__init__(self)
    self.x = 0

