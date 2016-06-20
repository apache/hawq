'this crashed pychecker from calendar.py in Python 2.2'

class X:
    'd'
    def test(self, item):
        return [e for e in item].__getslice__()

# this crashed in 2.2, but not 2.3
def f(a):
  a.a = [x for x in range(2) if x > 1]
