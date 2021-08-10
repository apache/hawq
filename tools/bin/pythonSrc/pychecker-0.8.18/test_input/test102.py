
"""This used to be a problem for code that assumed __bases__ existed."""

class GetattrReturnsNone(object):
  """This requires a new-style class."""
  def __getattr__(self, unused_name):
    return None

dummy1 = GetattrReturnsNone()

class GetattrReturnsStr(object):
  """This requires a new-style class."""
  def __getattr__(self, unused_name):
    return 'abc'

dummy2 = GetattrReturnsStr()
