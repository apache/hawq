'docstring'

def x(a, b):
    c = getattr(a, b)
    c = getattr(a, b, None)
    c = getattr(a, 'c')
    c = getattr(a, 'c', None)
    print c

    setattr(a, b, None)
    setattr(a, 'b', None)
