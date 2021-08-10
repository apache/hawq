
def f1(x):
    return '%s' % x

def f2(x):
    return ('%s' + '%s') % (x, x)

def f3(x):
    return (('%s' + '%s') * 8) % ((x,) * 16)

def f4():
    return '%(x)f %(y)s' % locals()

def f5():
    return '%' % locals()

def f6():
    return '%z this, too is a kookie format, yah' % locals()

def f7():
    return '%*.s %*.*s %*f' % locals()

def f8():
    return '%(mi)x %(it)s %up' % locals()

def f9():
    return '%s %d %f' % ('', 2)

def f10():
    return '%s %%' % ('',)

def f11(t):
    return '%s %f' % t

def f12(t):
    return '%s %lf' % (t, t)

def f13(t):
    return ('%s %f' + t) % (1, 2)

def f14():
    return '%up %(mi)x' % (1, 2)

def f15():
    return ('%s' * 6) % ((1, 2) + 3 * 7)

def f16():
    return '%(foo)*.*s' % {'foo': 'bar'}
