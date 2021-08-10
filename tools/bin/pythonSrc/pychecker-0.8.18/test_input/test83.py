'test unpacking variables'

def func1():
    'should not warn'
    x, z = (5, 2)

def func2():
    'should warn'
    x, y, z = (5, 2)

def func3():
    'should warn'
    a = (5, 2)
    x, y, z = a

def func4():
    'should warn'
    x, = (5, 2)

def func5():
    'should warn'
    x, y = 5,

def func6():
    'should warn'
    x, = 5

def func101():
    'should not warn'
    x, y, z = (5, 2, 3, 4, 5)[:3]

def func102():
    'should warn ideally, but size becomes unknown'
    x, y, z = (5, 2, 3, 4, 5)[:2]

def func103():
    'should not warn'
    x, y, z = (5, 2, 3, 4, 5)[2:]

def func104():
    'should warn ideally, but size becomes unknown'
    x, y, z = (5, 2, 3, 4, 5)[3:]

