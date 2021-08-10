
"global test"

def test1():
  'without -g option, this should be the only warning for x'
  global x
  x = 90

def test2():
  'with -g option, this should be also generate a warning for x'
  global x
  x = 90

def test3():
  'with -g option, this should be also generate a warning for x'
  global x
  x = 90

def test4():
  'without -g option, this should be the only warning for xxx'
  print xxx

def test5():
  'with -g option, this should be also generate a warning for xxx'
  print xxx

def test6():
  'with -g option, this should be also generate a warning for xxx'
  print xxx

