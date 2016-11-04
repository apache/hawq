from sys import path as foo, argv as bar# bar not used
import getopt                           # getopt not used
import os.path as nifty                 # nifty not used
from os.path import *

def f(v):
    from os.path import join            # duplicate import
    v.append(join(foo))                 # join from os.path
    v.append(__all__)                   # __all__ is unknown

x = 7
if x == 100:
    from xYzZy import *                 # not found
    x = z

not_used = 13
import getopt                           # duplicate import
from string import *                    # shadow 'join'

