'd'

def x():
    d = e = 7
    print d / e
    print d / 3
    print 7 / e
    print 5 / 6

def y1():
    x = 0
    return (x * 100.0) / 10

def y2():
    x = 0
    return (x + 100.0) / 10

def y3():
    x = 0
    return (x - 100.0) / 10

def y4():
    x = 1
    return (x ** 100.0) / 10

def z(e):
    x = 0
    if e:
        x = x + 1
        return (x * 100.0) / 10
    y = 0
    return (x * 100.0) / y

def should_not_warn(a):
    # this warning should be suppressed automatically
    x = int(a)
    return int(x / 5)
