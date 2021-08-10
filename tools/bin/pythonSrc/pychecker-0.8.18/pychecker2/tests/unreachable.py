v1=0

def f1():
    if v1:
        v2 = 2 + 3 * 4 / 7 << 3;
        return v2
    elif v2:
        a, b = 1, 2
        b, a = a, b
        return 0
    else:
        print 7
        return 1
    return 0                            # unreachable

def f2():
    if v1:
        return 0
    return 0

def f3():
    if v1:
        raise v1
    else:
        raise v1
    print v1                            # unreachable

def f4():
    assert None
    print v1                            # unreachable

def f5():
    assert 1
    print v1
    
def f6():
    assert 0
    print v1                            # unreachable


def f7():
    global v1
    try:
        assert None
    except KeyError:
        v1 = 7
    else:
        raise v1                        # unreachable
    return 0

def f8():
    try:
        print v1()
    except KeyError:
        return 7
    else:
        raise v1
    return 0                            # unreachable
