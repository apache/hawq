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
# Define ARCH values
#
#
case "`uname -s`" in
    Darwin)
        sw_vers | grep ProductVersion | grep 10.5
        if [ $? = 0 ]; then
            BLD_ARCH_HOST=osx105_x86
        else
            BLD_ARCH_HOST=osx106_x86
        fi
        ;;

    Linux)
    if [ -f /etc/redhat-release ]; then
        case "`cat /etc/redhat-release`" in
            "Red Hat Enterprise Linux WS release 3 (Taroon Update 4)")
            BLD_ARCH_HOST=rhel4_x86_32
            ;;
            *)
            BLD_ARCH_HOST="rhel`cat /etc/redhat-release | sed -e 's/Red Hat Enterprise Linux/RedHat/' -e 's/WS//' -e 's/Server//' -e 's/Client//' | awk '{print $3}' | awk -F. '{print $1}'`_`uname -p | sed -e s/i686/x86_32/`"
            ;;
        esac
    fi
    if [ -f /etc/SuSE-release ]; then
        case "`cat /etc/SuSE-release`" in
            *)
            BLD_ARCH_HOST="suse`head -1 /etc/SuSE-release | sed -e 's/SUSE Linux Enterprise Server/SuSE/' | awk '{print $2}'`_`uname -p | sed -e s/i686/x86_32/`"
            ;;
        esac
    fi
    ;;

    SunOS)
    BLD_ARCH_HOST="sol`uname -r | awk -F. ' {print $2}'`_`isainfo | awk ' {print $1}' | sed -e s/amd64/x86_64/ | sed -e s/sparcv9/sparc_64/`"
    ;;

    AIX)
    BLD_ARCH_HOST="aix`uname -v`_`prtconf | grep 'Processor Type' | awk '{print $3}' | perl -p -i -e 's,PowerPC_POWER5,ppc,'`_`prtconf -k | perl -p -i -e 's,Kernel Type: (\d+)-bit,\1,'`"
    ;;

    *)
    BLD_ARCH_HOST="BLD_ARCH_unknown"
    ;;
esac

echo ${BLD_ARCH_HOST}
