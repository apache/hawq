""
class Foo:
    ""
    def __init__(self,q):
        print q

class Bar(Foo):
    ""
    def __init__(self):
        Foo.__init__(self,{ 'a' : 1 })

