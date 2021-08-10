'test nested scopes'

from __future__ import nested_scopes

def x(p):
    def y():
       print p

    y()
    print p

def a(p):
    def y():
       print p

    y()

def chop(seq,size):
    chunk = lambda i: seq[i:i+size]
    return map(chunk,range(len(seq)))

