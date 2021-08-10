'''test using locals in module scope works and there are no spurious warnings'''

w = '%(u)s'
u = 'a'
result = w % locals()

