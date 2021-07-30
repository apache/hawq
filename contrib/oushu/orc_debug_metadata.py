#!/usr/bin/env python

import subprocess
import sys
import os
p = os.popen('orc-metadata --verbose ' + sys.argv[2]).read().split("\n")
f = ''
for cas in p:
    f += cas.replace('\n','')
print(sys.argv[1] + '|' + f)
