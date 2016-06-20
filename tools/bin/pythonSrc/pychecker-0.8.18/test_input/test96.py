'''test using string.find() in if statement as a boolean.  it returns an int'''

class X:
  '''check string.find() usage'''
  def foo(self):
    s = 'abc'
    if s.find('ab'):
       print 'this is a bug'
    if not s.find('ab'):
       print 'this is also a bug'
    if s.find('ab') >= 0:
       print 'this is not a bug'
    if s.find('ab') < 0:
       print 'this also is not a bug'

