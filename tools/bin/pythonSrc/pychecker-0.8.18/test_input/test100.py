# https://sourceforge.net/tracker/index.php?func=detail&aid=1563494&group_id=24686&atid=382217
# see zope.interface.declarations.Declaration
# for a real-world one line test, pycheck "import zope.interface.declarations"  

'd'
class D:
    'd'
    __bases__ = ()
    
    # this repr avoids getting the default repr, which shows an id
    # which changes between test runs
    __repr__ = classmethod(lambda cls: 'D')

# instantiating triggers the bug
d = D()
