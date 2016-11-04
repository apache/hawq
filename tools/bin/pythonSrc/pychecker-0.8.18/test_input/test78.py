'test using input()'

import readline # should not generate a warning since input/raw_input() is used

def f1():
    x = input()
    print x

def f2():
    x = raw_input()
    print x

def f3():
    'should not generate a warning, since this is not the real input'
    __pychecker__ = 'no-shadowbuiltin'
    def input(): return 0

    x = input()
    print x
