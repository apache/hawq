'test checking constant conditions'

# __pychecker__ = ''

def func1(x):
    'should not produce a warning'
    if 1:
        pass
    while 1:
        print x
        break
    assert x, 'test'
    return 0

def func2(x):
    'should produce a warning'
    __pychecker__ = 'constant1'
    if 1:
        pass
    while 1:
        print x
        break
    return 0

def func3(x):
    'should produce a warning'
    if 21:
        return 1
    if 31:
        return 2
    assert(x, 'test')
    assert(5, 'test')
    assert 5, 'test'

    if 'str':
        return 3

    return 4

def func4(x):
    'should not produce a warning'
    if x == 204 or x == 201 or 200 <= x < 300:
        x = 0
    if x == 1:
        pass
    while x == 'str':
        print x
        break
    return 0

def func5(need_quotes, text):
    'should not produce a warning'
    return (need_quotes) and ('"%s"' % text) or (text)

def func6(x):
    'should not produce warnings'
    if x & 32:
        print 'ya'
    if 32 & x:
        print 'ya'
