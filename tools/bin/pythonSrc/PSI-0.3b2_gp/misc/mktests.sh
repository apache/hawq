#!/bin/sh

# Quick and dirty script to test with lots of python versions.  This
# could grow many nice features (if it is the right place).


set -e


TESTED=


do_run() {
    py=$1

    echo
    echo ======== $py =========
    echo
    $py setup.py build_ext --devel
    $py setup.py test
    TESTED="$TESTED $py"
}


which python2.3 >/dev/null && do_run python2.3
which python2.4 >/dev/null && do_run python2.4
which python2.5 >/dev/null && do_run python2.5
which python2.6 >/dev/null && do_run python2.6
which python2.7 >/dev/null && do_run python2.7

which python3.0 >/dev/null && do_run python3.0
which python3.1 >/dev/null && do_run python3.1

echo
echo ======= Tested Against following pythons =======
for py in $TESTED; do
    $py -V
done