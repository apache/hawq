'doc'

class Newbie(object):
    '''__coerce__ is problematic in new style classes,
    see http://python.org/sf/547211
    '''
    def __coerce__(self, o): pass

