#!/bin/bash
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

if [ x$1 != x ] ; then
    GPHOME_PATH=$1
else
    GPHOME_PATH="\`pwd\`"
fi

if [ "$2" = "ISO" ] ; then
	cat <<-EOF
		if [ "\${BASH_SOURCE:0:1}" == "/" ]
		then
		    GPHOME=\`dirname "\$BASH_SOURCE"\`
		else
		    GPHOME=\`pwd\`/\`dirname "\$BASH_SOURCE"\`
		fi
	EOF
else
	cat <<-EOF
		GPHOME=${GPHOME_PATH}
	EOF
fi


PLAT=`uname -s`
if [ $? -ne 0 ] ; then
    echo "Error executing uname -s"
    exit 1
fi

cat << EOF

# Replace with symlink path if it is present and correct
if [ -h \${GPHOME}/../hawq ]; then
    GPHOME_BY_SYMLINK=\`(cd \${GPHOME}/../hawq/ && pwd -P)\`
    if [ x"\${GPHOME_BY_SYMLINK}" = x"\${GPHOME}" ]; then
        GPHOME=\`(cd \${GPHOME}/../hawq/ && pwd -L)\`/.
    fi
    unset GPHOME_BY_SYMLINK
fi
EOF

# OSX does NOT have DYLD_LIBRARY_PATH, add it
if [ "${PLAT}" = "Darwin" ] ; then
cat <<EOF
PATH=\$GPHOME/bin:\$PATH
DYLD_LIBRARY_PATH=\$GPHOME/lib:\$DYLD_LIBRARY_PATH
EOF
fi

# OSX does NOT need ext/python/bin/ path
if [ "${PLAT}" = "Darwin" ] ; then
cat <<EOF
PATH=\$GPHOME/bin:\$PATH
EOF
else
cat <<EOF
PATH=\$GPHOME/bin:\$GPHOME/ext/python/bin:\$PATH
EOF
fi

# OSX does NOT have LD_LIBRARY_PATH, add it
if [ "${PLAT}" != "Darwin" ] ; then
    #Solaris needs /usr/sfw/lib in order for groupsession to work and /usr/local/lib for readline for Python 
    if [ "${PLAT}" = "SunOS" ] ; then
    cat <<EOF
LD_LIBRARY_PATH=\$GPHOME/lib:\$GPHOME/ext/python/lib:/usr/sfw/lib:/usr/local/python/lib:\$LD_LIBRARY_PATH
EOF
    else
    cat <<EOF
LD_LIBRARY_PATH=\$GPHOME/lib:\$GPHOME/ext/python/lib:\$LD_LIBRARY_PATH
EOF
    fi
fi

#setup PYTHONPATH
# OSX does NOT need pygresql/ path
if [ "${PLAT}" = "Darwin" ] ; then
cat <<EOF
PYTHONPATH=\$GPHOME/lib/python:\$PYTHONPATH
EOF
else
cat <<EOF
PYTHONPATH=\$GPHOME/lib/python:\$GPHOME/lib/python/pygresql:\$PYTHONPATH
EOF
fi

# openssl configuration file path
cat <<EOF
OPENSSL_CONF=\$GPHOME/etc/openssl.cnf
EOF

# libhdfs3 configuration file path
cat << EOF
LIBHDFS3_CONF=\$GPHOME/etc/hdfs-client.xml
EOF

# libyarn configuration file path
cat << EOF
LIBYARN_CONF=\$GPHOME/etc/yarn-client.xml
EOF

# global resource manager configuration file path
cat << EOF
HAWQSITE_CONF=\$GPHOME/etc/hawq-site.xml
EOF

cat <<EOF
export GPHOME
export PATH
EOF

if [ "${PLAT}" != "Darwin" ] ; then
cat <<EOF
export LD_LIBRARY_PATH
EOF
else
cat <<EOF
export DYLD_LIBRARY_PATH
EOF
fi

cat <<EOF
export PYTHONPATH
EOF

cat <<EOF
export OPENSSL_CONF
EOF

cat <<EOF
export LIBHDFS3_CONF
EOF

cat <<EOF
export LIBYARN_CONF
EOF

cat <<EOF
export HAWQSITE_CONF
EOF
