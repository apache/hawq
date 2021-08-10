
if __name__ == "__main__":
    x = y

debug = 1
def _debug(msg):
    if debug:
        print msg

def main():
    import sys
    global debug
    debug = '-d' in sys.argv
    _debug('debug is %d' % debug)
