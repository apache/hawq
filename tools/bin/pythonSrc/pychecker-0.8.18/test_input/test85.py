'''test return (x and 'true' or 'false') idiom'''

def f1(test):
    'should not warn'
    return test and 'true' or 'false'

def f2(test):
    'should not warn about None'
    return test and 'true' or None

def f3(test):
    'should warn about None'
    return test and None or 'false'

def f4(test):
    'should warn'
    return test and [] or 0

def f5(test):
    'should not warn'
    return test and 'true' or 0

