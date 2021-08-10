# The MIT License
#
# Copyright (C) 2007 Chris Miles
#
# Copyright (C) 2008-2009 Floris Bruynooghe
#
# Copyright (C) 2008-2009 Abilisoft Ltd.
#
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Python System Information

PSI is a Python module providing direct access to real-time
system and process information.

See psi.arch for architecture information.  Use psi.arch.arch_type()
to fetch an object representing the architecture that the Python
interpreter is running on.

See psi.process module for access to run-time process information.
"""


from psi._psi import AttrNotAvailableError, AttrInsufficientPrivsError, \
    AttrNotImplementedError, MissingResourceError
from psi._psi import TimeSpec
from psi._psi import boottime, loadavg, uptime
try:
    from psi._psi import getzoneid, getzoneidbyname, getzonenamebyid
except ImportError:
    pass

from psi._version import version as __version__
from psi._version import author as __author__
from psi._version import copyright as __copyright__
from psi._version import license as __license__
