
"doc"

import getopt

def func():
    "re-importing getopt should complain"
    import getopt
    print getopt

def test():
    "this should fail (there is no getopt.xyz)"
    try:
        print ""
    except getopt.xyz:
        pass

