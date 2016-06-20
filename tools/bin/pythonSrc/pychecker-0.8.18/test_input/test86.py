'warn about raising/catching string exceptions'

__pychecker__ = 'no-classdoc'

class g(Exception): pass
h = 'blah'
i = g
j = KeyError
class k: pass

def f1(a, b, c):
    'should not warn'
    class z(Exception): pass
    if a:
        raise
    if a:
        raise KeyError
    if a:
        raise b, ''
    if a:
        raise c()
    if a:
        raise g, ''
    if a:
        raise i
    if a:
        raise j
    if a:
        raise z

def f2(a):
    'should warn'
    if a:
        raise 'strerr'
    if a:
        raise h, ''
    if a:
        raise k

def f3(a, b, c):
    'should not warn'
    class z(Exception): pass
    try:
        f1(a, b, c)
    except a:
        pass
    except b.bbb:
        pass
    except KeyError:
        pass
    except (TypeError, KeyError):
        pass
    except g:
        pass
    except z:
        pass
    except (g, z):
        pass
    except i, detail:
        print detail
    except Exception, detail:
        print detail
    except:
        pass

def f4(a):
    'should warn'
    try:
        f2(a)
    except 'strerr':
        pass
    except ('strerr1', 'strerr2'):
        pass
    except ('strerr1', KeyError):
        pass
    except h:
        pass
    except k:
        pass

tt = (KeyError, ValueError)

def f5(a):
    'should not warn'
    try:
        f2(a)
    except tt:
        pass

def f6(a):
    'should not warn'
    # but does in 0.8.17
    try:
        pass
    except KeyboardInterrupt:
        pass
