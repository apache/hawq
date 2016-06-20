'this test will fail unless -r/--returnvalues is used'

def func0(x):
    'should not be a warning'
    return 0

def func1(x):
    'should be a warning implicit/explicit'
    if x == 0 :
        return 1

def func2(x):
    'should not be a warning'
    if x == 0 :
        return

def func3(x):
    'should be a warning'
    if x == 0 :
        return 1, 2
    return 1

def func4(x):
    'should not be a warning, difficult to deal with'
    c = (1, 2)
    if x == 0 :
        return c
    return 9, 8

def func5(x):
    'should not be a warning'
    if x == 0 :
        return 1, 2
    return 9, 8

def func6(x):
    'should be a warning'
    if x == 0 :
        return 1, 2
    return 9, 8, 7

