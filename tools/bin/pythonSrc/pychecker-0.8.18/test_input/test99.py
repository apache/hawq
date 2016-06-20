"""None of these should blow up."""

class Foo:
  """Base class to call during test"""
  def __init__(self, *args, **kwargs): pass

A = B = C = D = 1

class AndBar(Foo):
  """Base class to call during test"""
  def __init__(self):
    Foo.__init__(self, style = A & B & C & D)

class OrBar(Foo):
  """Base class to call during test"""
  def __init__(self):
    Foo.__init__(self, style = A | B | C | D)

class XorBar(Foo):
  """Base class to call during test"""
  def __init__(self):
    Foo.__init__(self, style = A ^ B ^ C ^ D)
