'd'

def x():
    try :
        print "howdy, this ain't right"
    except KeyError, RuntimeError :
        pass

def y():
    try :
        print "ok, " + "this func %s should be fine" % y.__name__
    except (KeyError, RuntimeError) :
        pass

def z():
    try :
        pass
    except (KeyError, RuntimeError, IndexError) :
        pass

def a():
    try :
        pass
    except (KeyError, RuntimeError, IndexError), a :
        print a

try :
    pass
except KeyError, RuntimeError :
    pass

try :
    pass
except (KeyError, RuntimeError) :
    pass

def b():
    try :
        print "ok, " + "this func %s should be fine" % y.__name__
    except (KeyError, RuntimeError), msg :
        print msg
def c():
    try :
        print "ok, " + "this func %s should be fine" % y.__name__
    except KeyError, detail :
        print detail

