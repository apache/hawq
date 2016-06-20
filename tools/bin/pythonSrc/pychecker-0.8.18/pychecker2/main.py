import sys
from os.path import dirname, realpath
sys.path.append(dirname(dirname(realpath(sys.argv[0]))))

from pychecker2.Check import CheckList

from pychecker2 import Options
from pychecker2 import ParseChecks
from pychecker2 import OpChecks
from pychecker2 import VariableChecks
from pychecker2 import ScopeChecks
from pychecker2 import ImportChecks
from pychecker2 import ClassChecks
from pychecker2 import ReachableChecks
from pychecker2 import ReturnChecks
from pychecker2 import ConditionalChecks
from pychecker2 import FormatStringChecks

CACHE_FILE = '/tmp/t'

def print_warnings(f, out):
    if not f.warnings:
        return 0
    f.warnings.sort()
    last_line = -1
    last_msg = None
    for line, warning, args in f.warnings:
        if warning.value:
            msg = warning.message % args
            if msg != last_msg or line != last_line:
                print >>out, \
                      '%s:%s %s' % (f.name, line or '[unknown line]', msg)
                last_msg, last_line = msg, line
    if last_msg:
        print >>out
    return 1

def create_checklist(options):

    checks = [ ParseChecks.ParseCheck(),
               OpChecks.OpCheck(),
               OpChecks.ExceptCheck(),
               OpChecks.CompareCheck(),
               ReachableChecks.ReachableCheck(),
               ConditionalChecks.ConstantCheck(),
               ClassChecks.ReprCheck(),
               ImportChecks.ImportCheck(),
               FormatStringChecks.FormatStringCheck(),
               VariableChecks.ShadowCheck(),
               VariableChecks.UnpackCheck(),
               VariableChecks.UnusedCheck(),
               VariableChecks.UnknownCheck(),
               VariableChecks.SelfCheck(),
               VariableChecks.UsedBeforeSetCheck(),
               ReturnChecks.MixedReturnCheck(),
               ClassChecks.AttributeCheck(),
               ClassChecks.SpecialCheck(),
               ClassChecks.InitCheck(),
               ScopeChecks.RedefineCheck(),
               ]
    for checker in checks:
        checker.get_warnings(options)
        checker.get_options(options)
    return CheckList(checks)

def main():
    import cPickle
    
    options = Options.Options()
    try:
        checker = cPickle.load(open(CACHE_FILE, 'rb'))
    except (EOFError, IOError):
        checker = create_checklist(options)

    try:
        files = options.process_options(sys.argv[1:])
    except Options.Error, detail:
        print >> sys.stderr, "Error: %s" % detail
        options.usage(sys.argv[0], sys.stderr)
        return 1

    for f in files:
        checker.check_file(f)
        if options.incremental and not options.profile:
            print_warnings(f, sys.stdout)

    result = 0
    if not options.incremental and not options.profile:
        files.sort()
        for f in files:
            result |=  print_warnings(f, sys.stdout)

        if not result and options.verbose:
            print >>sys.stdout, None

    fp = open(CACHE_FILE, 'wb')
    cPickle.dump(checker, fp, 1)
    fp.close()

    return result

if __name__ == "__main__":
    if '--profile' in sys.argv:
        print 'profiling'
        import hotshot.stats
        import time
        hs = hotshot.Profile('logfile.dat')
        now = time.time()
        hs.run('main()')
        print 'total run time', time.time() - now
        hs.close()
        stats = hotshot.stats.load('logfile.dat')
        stats.sort_stats('time', 'cum').print_stats(50)
    else:
        sys.exit(main())
