"doesn't work in 1.5.2"

def ttt(c):
  return None

def x():
  "should not complain about either, we can't check # args"
  columns = [ 1, 2 ]
  print zip(*columns)
  print ttt(*columns)

