'test using is/is not on literals'

def x(a, b, c):
    print a is 1
    print a is 's'
    print a is 1.32
    print a is ()
    print a is []
    print a is {}
    print a is b
    print b is c
    print b is None

    print a is not 1
    print a is not 's'
    print a is not 1.32
    print a is not ()
    print a is not []
    print a is not {}
    print a is not b
    print b is not c
    print b is not None
