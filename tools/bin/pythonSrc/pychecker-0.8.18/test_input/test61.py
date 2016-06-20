'd'

class X:
  'd'
  def x(self, g, h): pass

def y():
  x = X()
  print x.x()

def z():
  x = X()
  print x.x(1, 2)

def a():
  d = {}
  print d.get(1, 5)
  print d.get(1, 5, 7)
  print d.keys()
  print d.keys(1, 5, 7)
  print d.clear()
  x = d.clear()
  x.y = d.clear()
  d.clear()
  print
  print setattr(d, 'junk', 'neal')
  print `d`.rjust(5)

