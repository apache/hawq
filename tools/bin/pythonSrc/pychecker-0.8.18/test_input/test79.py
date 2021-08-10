'test iterating over a string'

__pychecker__ = 'stringiter'

def func1():
    'should generate a warning'
    s = 'string'
    for c in s:
        print 'oops', c[0]

def func2():
    'should generate a warning'
    f = open('/dev/null')
    s = f.read()
    for c in s:
        print 'oops', c[0]

def func3():
    'should not generate a warning'
    for i in []:
        print 'ok', i
    for i in ():
        print 'ok', i

def func4():
    'should not generate a warning'
    v = []
    for i in v:
        print 'ok', i
    w = ()
    for i in w:
        print 'ok', i

