'test setting class attributes not in __init__ w/initattr set'

__pychecker__ = 'initattr no-classdoc'

class A:
  'd'
  def a(self):
    self.y = 0

class B(A):
  'd'
  def b(self):
    self.z = 0

class C:
  'd'
  def __init__(self):
    self.a = self.b = self.c = 0
    self.d = None
    self.e = []

  def b(self):
    print self.z
    self.y = 0

class D(C):
  def c(self):
    self.a = 5

