'test implicit returns'

__pychecker__ = 'implicitreturns'

def func1(x):
    'should not produce a warning'
    if x == 1:
        return 1
    return 0

def func2(x):
    'should produce a warning'
    if x == 1:
        return 1

def func3(x):
    'should not produce a warning'
    while 1:
        if x == 1:
            return 1
        x = x / 2
    return 0

def func4(x):
    'should not produce a warning'
    while 1:
        if x == 1:
            return 1
        x = x / 2

def func5(x):
    'should not produce a warning'
    while 1:
        if x == 1:
            return 1
    return 0

def func6(x):
    'should produce a warning'
    while 1:
        if x == 1:
            return 1
        break

def func7(x):
    'should not produce a warning'
    try:
        print x
        return 2
    except:
        pass
    return 0

def func8(x):
    'should produce a warning'
    try:
        if x == 1:
            return 3
        if x == 2:
            return 6
    except:
        pass

def func9(x):
    'should not produce a warning'
    try:
        return x
    except:
        return 0

def func10(x):
    'should not produce a warning'
    if x:
        raise ValueError

def func11(x):
    'should not produce a warning'
    if x:
        raise ValueError
    return 5

def func12(x):
    'should not produce a warning'
    raise ValueError, 'test'

def func13(x):
    'should not produce a warning'
    if x == 1:
        return 1
    else:
        return 0

def func14(x):
    'should not produce a warning'
    try:
        if x == 1:
            return 3
        return 6
    except:
        raise

def func15(x):
    'should not produce a warning'
    try:
        return x.j
    except AttributeError:
        return 0

def func16(x):
    'should not produce a warning'
    try:
        return x.j
    except AttributeError:
        raise

def func17(x):
    'should not produce a warning'
    try:
        return x.j
    except (AttributeError, KeyError, IndexError):
        return 0

def func18(x):
    if x == 'n':
        return x
    if x != 'j':
        raise AttributeError

def func19(x):
    'should not produce a warning'
    while 1:
        if x:
            x = x + 1
        return 1

def func20(x):
    'should produce a warning'
    while 1:
        if x:
            break
        return 1

def func21(x):
    'should not produce a warning'
    try:
        if x == 1:
            return 3
        return 6
    finally:
        print 'do nothing'

def func22(x):
    'should not produce a warning'
    while 1:
        for _ in range(10) :
            x = x / 2
            break
        return 1

def catchup(slave, image, inProgress):
    d = func1.bogus()
    def next_func():
        defer = slave.call('', image.next())
        try:
            defer.add(d.errback)
        except:
            slave.call(inProgress)
    next_func()
    return d

