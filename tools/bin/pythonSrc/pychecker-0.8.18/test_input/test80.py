'test inconsistent types'

__pychecker__ = 'changetypes'

def func1():
    'should generate a warning'
    x = 0
    x += 1.1
    print x

def func2():
    'should not generate a warning'
    x = ""
    x = "string"
    for j in []:
        x = j
    print x

def func3():
    'should generate a warning'
    y = None
    y = []
    y = {}
    print y

