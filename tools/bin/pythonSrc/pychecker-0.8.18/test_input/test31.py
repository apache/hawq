'doc'

class X:
  'doc'

class Y(X):
  'doc'

class Z(X):
  'doc'
  def __init__(self, arg):
    pass

def x():
  'should not cause a warning'
  print X()
  print Y()
  print Z('should create a warning since no __init__()')

def y():
  print X(a='should create a warning since no __init__()')
  print X('should create a warning since no __init__()')
  print Y(a='should create a warning since no __init__()')
  print Y('should create a warning since no __init__()')
  print Z(1, a='should create a warning since no __init__()')
  print Z(1, 2)

class A:
  'd'
  def a(self, a, b, c): pass
  def x(self, a, b, c): pass

class B(A):
  'd'
  def y(self):
      A.x(self, 1, 2, 3)
  def z(self):
      A.a(self, 1, 2, 3)
