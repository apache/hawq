'test use of True/False'

True = 1
False = 0

true = 1
false = 0

TRUE = 1
FALSE = 0

def x(a):
    'should warn'
    if a == TRUE:
        print 'True'
    if a == TRUE or \
       a == True or \
       a == true:
        print 'True'
    if a != false:
        print 'false'
    if a is 'false':
        print 'false'
    if a is 'True':
        print 'True'
    if a is 53343:
        print '53343'
    if a is 1e7:
        print '1e7'
    if a is not 'false':
        print 'false'
    if a is not 'True':
        print 'True'
    if a is not 53343:
        print '53343'
    if a is not 1e7:
        print '1e7'

def y(a):
    'should not warn'
    if a is True:
        print 'True'
    if a is not false:
        print 'false'
    if a == 53343:
        print '53343'
    if a == 1e7:
        print '1e7'
    if a == 'TRUE' or \
       a == 'True' or \
       a == 'true':
        print 'True'

def z(a):
    return True

