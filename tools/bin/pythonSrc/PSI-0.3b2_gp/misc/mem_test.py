"""Check for memory leak

If the memory goes consistent and significantly up there is a leak,
either directly or from reference counting errors.
"""


import datetime
import os
import sys
import time

import psi
import psi.arch
import psi.process
import psi.mount


def check_psi():
    psi.loadavg()
    psi.boottime()
    psi.uptime()


def check_arch():
    psi.arch.ArchBase()


def check_process(pid):
    p = psi.process.Process(pid)
    p.children()
    p.exists()
    p.refresh()
    psi.process.ProcessTable()


def check_mount():
    for m in psi.mount.mounts():
        pass


def loop(count=300):
    pid = os.getpid()
    print 'pid:', os.getpid()
    p = psi.process.Process(os.getpid())
    startrss = p.rss
    starttime = datetime.datetime.now()
    tdiff = datetime.datetime.now() - starttime
    n = 0
    while n < count:
        check_psi()
        check_arch()
        check_process(pid)
        p.refresh()
        tdiff = datetime.datetime.now() - starttime
        print 'time: %(time)s rss: %(rss)s (%(pct)s%%)' \
            % {'rss': p.rss,
               'pct': p.rss*100/startrss,
               'time': str(tdiff).split('.')[0]}
        n += 1
    p = psi.process.Process(os.getpid())
    print 'n=%(n)d - rss: %(rss)s (%(pct)s%%)' \
        % {'rss': p.rss,
           'pct': p.rss*100/startrss,
           'n': n}


if __name__ == '__main__':
    if len(sys.argv) > 1:
        loop(int(sys.argv[1]))
    else:
        loop()
