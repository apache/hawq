"doc"

class Test:
    "'No attribute (alias) found' warning should not be generated"

    def __init__(self):
        self.alias()

    def real(self):
        pass

    alias = real


def rcParseTest(a = 10, b = 'ten', c = (5, 5)):
    print '%s, %s, %s' % (a, b, str(c))

def a(i):
    [0, 1, 3].index(i)

