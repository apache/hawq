#!/bin/sh

TESTS="test_input/test[1-9]*.py"
# comment out to use python from path
#PYTHON="python2"
#PYTHON="$HOME/build/python/2_3/python"
#PYTHON="$PYTHON -tt coverage.py -x"
#PYTHON="/usr/bin/python2.3"

if [ "$PYTHON" = "" ]; then
    PYTHON=python
fi

if [ $# -gt 0 ]; then
    TESTS=""
    for arg in $* ; do
        TESTS="$TESTS test_input/test${arg}.py"
    done
fi

if [ -z "$TMP" ]; then
    TMP=/tmp
fi

error=0

VERSION=`$PYTHON -c "import sys ; print '%d.%d' % sys.version_info[0:2]"`
FAILED=""
NO_EXPECTED_RESULTS=""
for test_file in $TESTS ; do
    echo "Testing $test_file ..."
    test_name=`basename $test_file .py`
    expected=test_expected/$test_name
    if [ -e ${expected}-$VERSION ]; then
        expected=${expected}-$VERSION
    elif [ ! -e $expected ]; then
        echo "  WARNING:  $expected expected results does not exist"
        NO_EXPECTED_RESULTS="$NO_EXPECTED_RESULTS $test_name"
        continue
    fi

    # make sure to use the -F option for this special test
    extra_args=""
    if [ "$test_file" = "test_input/test39.py" ]; then
        extra_args="-F test_input/pycheckrc"
    fi

    test_path=$TMP/$test_name
    $PYTHON -tt ./pychecker/checker.py --limit 0 --moduledoc --classdoc --no-argsused $extra_args $test_file 2>&1 | egrep -v '\[[0-9]+ refs\]$' > $test_path
    diff $test_path $expected
    if [ $? -ne 0 ]; then
        error=`expr $error + 1`
        echo "  $test_name FAILED"
        FAILED="$FAILED $test_name"
    fi
    rm -f $test_path
done

if [ $error -ne 0 ]; then
    echo ""
    echo "$errors TESTS FAILED: $FAILED"
else
    echo "ALL TESTS PASSED"
fi

if [ "$NO_EXPECTED_RESULTS" != "" ]; then
    echo " WARNING no expected results for: $NO_EXPECTED_RESULTS"
fi

