'd'

def y():
    print '%d %f %s' % (1, 2.2, 'sdf')

    print '%d %f %s %d' % (1, 2.2, 'sdf')

    aaa = bbb = 1
    eee = 0
    print '%(aaa)d %(bbb)f %(ccc)s %(ddd)s' % locals()

    b = 0
    print '%()s %(b)d' % locals()

    print '%(b) %(aaa)d' % locals()
    print '%(aaa)d %(b)' % locals()

    print '%*d' % (2, 2)
    print '%*d' % (2, 2, 3)

    print '%*.*f' % (5, 2, 2.0)
    print '%*.*f' % (5, 2, 2.0, 3)

    print '%z %f %s' % (1, 2.2, 'sdf')
    print '%d %J %s' % (1, 2.2, 'sdf')
    print '%***f' % (5, 2, 2.0, 3)

    print '%(aaa)d %d' % locals()
    print '%(aaa)*d' % locals()
    jjj = 1.0
    print '%(jjj)*.*f' % locals()

    fmt = '%d %s %d'
    print fmt % (aaa, bbb)


_F = '%d %d'

def ZZ():
    print _F % 5
    print _F % (5, 5)
    print _F % (5, 5, 5)

    F = '%d'
    print F % 5
    print F % (5, 5, 5)

    t1 = (1,)
    t2 = (1,2)
    print F % t1
    print F % t2

def YY(item):
    print '%(a)s %(b)s' % { 'a': '5', 'b': '7' }
    print '%(a)s %(b)s' % item
    d = { 'a': '5', 'b': '7' }
    print '%(a)s %(b)s' % d

def string_multiply():
    # these are right
    print '%d ' * 3 % (1, 2, 3)
    print ('%d ' * 3) % (1, 2, 3)

    # these are wrong
    print '%d ' * 2 % (1, 2, 3)
    print '%d ' * 3 % (1, 2, 3, 4)
    print ('%d ' * 4) % (1, 2)

def check_vars1():
    a = b = c = 5
    print '%(a)d %(b)d' % vars()

def check_vars2():
    a = b = c = 5
    print '%(a)d %(b)d' % vars(c)

def check_modulo_1():
  print '%s' % '1'
  print '%s' % 1
  print .55555 % 1
  print 55555 % 1
  f = 55555.55 
  print f % 1

def check_bad_format_end_percent(arg):
  print 'ha, better not crash %' % arg
