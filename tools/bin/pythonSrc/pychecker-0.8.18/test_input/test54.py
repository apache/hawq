'd'

real_global = 0

def f(a, b, c):
    del a
    del z
    z = 0
    del a
    del z

    print a, b, c

    global no_global
    del no_global

if __name__ == '__main__':
    del another_non_existant_global
    del real_global

