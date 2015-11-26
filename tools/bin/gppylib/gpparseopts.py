#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Standard set of helper functions for gp utilities for parsing command line options.



"""
from optparse import Option, OptionGroup, OptionParser, OptionValueError, SUPPRESS_USAGE, SUPPRESS_HELP
import os
import os.path
import re
import sys

from logfilter import MatchRegex, NoMatchRegex, filterize, MatchColumns
from gppylib.datetimeutils import str_to_datetime, str_to_duration, DatetimeValueError


# Local global value indicating whether regular expressions handled by the 
# option parser should be case sensitive or not.
# 
# Valid values are 'respect' or 'ignore'
_gCase = 'respect'

#----------------------- Command line option parser ----------------------

    
class OptParser(OptionParser):
    
    
    # OptionParser's help text formatter removes paragraph breaks and
    # preserves excess spaces.  Override its (undocumented) format_help
    # method so our help can have paragraphs.
    def format_help(self, *args):
        msg = OptionParser.format_help(self, *args)
        helptup = [self.formatter.format_description(' '.join(s.split()))
                   for s in self.helpStr]
        msg = '\n'.join([msg] + helptup)
        return msg

    def setHelp(self,helpStr):
        self.helpStr=helpStr
    
        
    def print_help(self, outfile=None):
        helpFileStr=self.read_helpfile()
        if helpFileStr:
            if outfile is None:
                outfile = sys.stdout
            outfile.write(helpFileStr)
        else:
            OptionParser.print_help(self,outfile)
            
    
    def read_helpfile(self):
        progname = os.path.split(sys.argv[0])[-1]
        help_path = os.path.join(sys.path[0], '..', 'docs', 'cli_help', progname + '_help')
        f = None
        try:
            try:
                f = open(help_path);
                return f.read(-1)
            except:
                return None
        finally:
            if f: f.close()  
              
        
class OptChecker(Option):
    # Teach optparse to accept types 'datetime', 'duration', 'regex', 'literal'
    def datetimeCheck(option, opt, value):
        try:
            return str_to_datetime(value)
        except DatetimeValueError, e:
            raise OptionValueError('"%s %s" ... %s' % (opt, value, e.description))
            
    def durationCheck(option, opt, value):
        try:
            return str_to_duration(value, signed=False)
        except DatetimeValueError, e:
            raise OptionValueError('"%s %s" ... %s' % (opt, value, e.description))
            
    def regexCheck(option, opt, value):
        # value is a string to be compiled as a regular expression pattern
        global _gCase
        flags = re.LOCALE
        if _gCase and _gCase.startswith('i'):
            flags |= re.IGNORECASE
        try:
            return re.compile(value, flags)
        except Exception, e:
            raise OptionValueError('"%s %s" ... %s' % (opt, value, e))
 
    def regexSetCaseSensitivity(option, opt, value, parser):
        """
        This is used to set the case sensitivity of optparser regex matching
        that occur after this option is set.

        Valid values are ['respect', 'r', 'ignore', 'i']

        A typical usage might look like this:
            parser.add_option('-c', action='callback', 
            callback=OptChecker.regexSetCaseSensitivity)
        """
        global _gCase
        _gCase = value
            
    def literalCheck(option, opt, value):
        # value is a string to be matched literally; return compiled regex
        return option.regexCheck(opt, re.escape(value))
        
    _addtypes = {'datetime' : datetimeCheck,
                 'duration' : durationCheck,
                 'regex'    : regexCheck,
                 'literal'  : literalCheck}
                 
    TYPES = Option.TYPES + tuple(_addtypes.keys())
    TYPE_CHECKER = Option.TYPE_CHECKER.copy()
    TYPE_CHECKER.update(_addtypes)

    # Teach optparse some additional actions
    def filterAction(self, dest, opt, value, values, parser, Filter, *args):
        filterlist = values.ensure_value(dest, [])
        filterlist.append(filterize(Filter, value, *args))
    
    def brief_help(self, dest, opt, value, values, parser, Filter, *args):
        OptionParser.print_help(parser,None)
        parser.exit()
        
    def optionalSecondArgAction(self, dest, opt, value, values, parser, *args):
        # If the next arg converts successfully to the proper type, consume it.
        # Store pair (value, 2nd value or None) into the destination.
        value2 = None
        if parser.rargs:
            nextarg = parser.rargs.pop(0)
            try:
                value2 = self.convert_value(opt, nextarg)
            except Exception:
                parser.rargs.insert(0, nextarg)
        setattr(values, dest, (value, value2))
    
        
        
    def take_action(self, action, dest, opt, value, values, parser):
        actup = self._addactions.get(action)
        if actup:
            actup[0](self, dest, opt, value, values, parser, actup[1], *actup[2:])
        else:
            Option.take_action(self, action, dest, opt, value, values, parser)
            
            
    _addactions = {"MatchRegex"        : (filterAction, MatchRegex),
                   "NoMatchRegex"      : (filterAction, NoMatchRegex),
                   "MatchColumns"      : (filterAction, MatchColumns),
                   "briefhelp"         : (brief_help,None),
                   "optionalSecondArg" : (optionalSecondArgAction, None)}
                   
                   
    _addactionnames = tuple(_addactions.keys())
    ACTIONS = Option.ACTIONS + _addactionnames
    STORE_ACTIONS = Option.STORE_ACTIONS + _addactionnames
    TYPED_ACTIONS = Option.TYPED_ACTIONS + _addactionnames

