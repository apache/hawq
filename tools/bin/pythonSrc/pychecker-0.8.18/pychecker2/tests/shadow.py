
def f(a, b):
    a = b
    for a in range(10):
        b += a
    return b

def f(a, b):
    return a + b

f = 3

max = 3

def g(a, b):
    def f(c, d):
        a = c
        b = d + max
    return f(c, d)

class C:
    f = lambda a, b: a + b
    def f(self, b):
        pass

class D:
    CONSTANT = 1
    def h(self, arg):
        CONSTANT = D.CONSTANT
        arg.append(CONSTANT)

def xyzzy(a, max=max, *args, **kw):
    print a, args, kw
    return max

if __name__ == '__main__':
    a = 2
if '__main__' == __name__:
    a = 2

if x:
    def f1():
        print x
else:
    def f1():
        print x

try:
    def f2():
        print x
except AttributeError:
    def f2():
        print x
except:
    def f2():
        print x
else:
    def f2():                           # should still warn
        print x
