'test 3+ nested functions'

__pychecker__ = 'argsused'

def x(a=1):
    def y(b=2, c=3, d=4):
        def z(b=2, e=5):
            print 'in z', b, e
        print 'in y', b, c
        z()
    print 'in x'
    y()
