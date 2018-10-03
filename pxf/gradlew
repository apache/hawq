#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

##############################################################################
##
##  Gradle start up script for UN*X
##
##############################################################################

set -e
set -o pipefail

APP_NAME="Gradle"
APP_BASE_NAME=`basename "$0"`

# Use the maximum available, or set MAX_FD != -1 to use that value.
MAX_FD="maximum"

# Add default JVM options here. You can also use JAVA_OPTS and GRADLE_OPTS to pass JVM options to this script.
DEFAULT_JVM_OPTS=""

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

if [ -e "$bin/gradle/wrapper/gradle-wrapper.properties" ]; then
  . "$bin/gradle/wrapper/gradle-wrapper.properties"
else
  # the location that the wrapper is at doesn't have a properties
  # check PWD, gradlew may be shared
  if [ -e "$PWD/gradle/wrapper/gradle-wrapper.properties" ]; then
    . "$PWD/gradle/wrapper/gradle-wrapper.properties"
  else
    echo "Unable to locate gradle-wrapper.properties.  Not at $PWD/gradle/wrapper/gradle-wrapper.properties or $bin/gradle/wrapper/gradle-wrapper.properties" 1>&2
    exit 1
  fi
fi

warn () {
    echo "$*"
}

die () {
    echo
    echo "$*"
    echo
    exit 1
}

# OS specific support (must be 'true' or 'false').
cygwin=false
msys=false
darwin=false
nonstop=false
case "`uname`" in
  CYGWIN* )
    cygwin=true
    ;;
  Darwin* )
    darwin=true
    ;;
  MINGW* )
    msys=true
    ;;
  NONSTOP* )
    nonstop=true
    ;;
esac

# Attempt to set APP_HOME
# Resolve links: $0 may be a link
PRG="$0"
# Need this for relative symlinks.
while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
        PRG="$link"
    else
        PRG=`dirname "$PRG"`"/$link"
    fi
done
SAVED="`pwd`"
cd "`dirname \"$PRG\"`/" >/dev/null
APP_HOME="`pwd -P`"
cd "$SAVED" >/dev/null

CLASSPATH=$APP_HOME/gradle/wrapper/gradle-wrapper.jar

# Determine the Java command to use to start the JVM.
if [ -n "$JAVA_HOME" ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
        # IBM's JDK on AIX uses strange locations for the executables
        JAVACMD="$JAVA_HOME/jre/sh/java"
    else
        JAVACMD="$JAVA_HOME/bin/java"
    fi
    if [ ! -x "$JAVACMD" ] ; then
        die "ERROR: JAVA_HOME is set to an invalid directory: $JAVA_HOME

Please set the JAVA_HOME variable in your environment to match the
location of your Java installation."
    fi
else
    JAVACMD="java"
    which java >/dev/null 2>&1 || die "ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.

Please set the JAVA_HOME variable in your environment to match the
location of your Java installation."
fi

# Increase the maximum file descriptors if we can.
if [ "$cygwin" = "false" -a "$darwin" = "false" -a "$nonstop" = "false" ] ; then
    MAX_FD_LIMIT=`ulimit -H -n`
    if [ $? -eq 0 ] ; then
        if [ "$MAX_FD" = "maximum" -o "$MAX_FD" = "max" ] ; then
            MAX_FD="$MAX_FD_LIMIT"
        fi
        ulimit -n $MAX_FD
        if [ $? -ne 0 ] ; then
            warn "Could not set maximum file descriptor limit: $MAX_FD"
        fi
    else
        warn "Could not query maximum file descriptor limit: $MAX_FD_LIMIT"
    fi
fi

# For Darwin, add options to specify how the application appears in the dock
if $darwin; then
    GRADLE_OPTS="$GRADLE_OPTS \"-Xdock:name=$APP_NAME\" \"-Xdock:icon=$APP_HOME/media/gradle.icns\""
fi

# For Cygwin, switch paths to Windows format before running java
if $cygwin ; then
    APP_HOME=`cygpath --path --mixed "$APP_HOME"`
    CLASSPATH=`cygpath --path --mixed "$CLASSPATH"`
    JAVACMD=`cygpath --unix "$JAVACMD"`

    # We build the pattern for arguments to be converted via cygpath
    ROOTDIRSRAW=`find -L / -maxdepth 1 -mindepth 1 -type d 2>/dev/null`
    SEP=""
    for dir in $ROOTDIRSRAW ; do
        ROOTDIRS="$ROOTDIRS$SEP$dir"
        SEP="|"
    done
    OURCYGPATTERN="(^($ROOTDIRS))"
    # Add a user-defined pattern to the cygpath arguments
    if [ "$GRADLE_CYGPATTERN" != "" ] ; then
        OURCYGPATTERN="$OURCYGPATTERN|($GRADLE_CYGPATTERN)"
    fi
    # Now convert the arguments - kludge to limit ourselves to /bin/sh
    i=0
    for arg in "$@" ; do
        CHECK=`echo "$arg"|egrep -c "$OURCYGPATTERN" -`
        CHECK2=`echo "$arg"|egrep -c "^-"`                                 ### Determine if an option

        if [ $CHECK -ne 0 ] && [ $CHECK2 -eq 0 ] ; then                    ### Added a condition
            eval `echo args$i`=`cygpath --path --ignore --mixed "$arg"`
        else
            eval `echo args$i`="\"$arg\""
        fi
        i=$((i+1))
    done
    case $i in
        (0) set -- ;;
        (1) set -- "$args0" ;;
        (2) set -- "$args0" "$args1" ;;
        (3) set -- "$args0" "$args1" "$args2" ;;
        (4) set -- "$args0" "$args1" "$args2" "$args3" ;;
        (5) set -- "$args0" "$args1" "$args2" "$args3" "$args4" ;;
        (6) set -- "$args0" "$args1" "$args2" "$args3" "$args4" "$args5" ;;
        (7) set -- "$args0" "$args1" "$args2" "$args3" "$args4" "$args5" "$args6" ;;
        (8) set -- "$args0" "$args1" "$args2" "$args3" "$args4" "$args5" "$args6" "$args7" ;;
        (9) set -- "$args0" "$args1" "$args2" "$args3" "$args4" "$args5" "$args6" "$args7" "$args8" ;;
    esac
fi

# does not match gradle's hash
# waiting for http://stackoverflow.com/questions/26642077/java-biginteger-in-bash-rewrite-gradlew
hash() {
  local input="$1"
  if $darwin; then
    md5 -q -s "$1"
  else
    echo -n "$1" | md5sum  | cut -d" " -f1
  fi
}

dist_path() {
  local dir=$(basename $distributionUrl | sed 's;.zip;;g')
  local id=$(hash "$distributionUrl")

  echo "$HOME/.gradle/${distributionPath:-wrapper/dists}/$dir/$id"
}

zip_path() {
  local dir=$(basename $distributionUrl | sed 's;.zip;;g')
  local id=$(hash "$distributionUrl")

  echo "$HOME/.gradle/${zipStorePath:-wrapper/dists}/$dir/$id"
}

download() {
  local base_path=$(dist_path)
  local file_name=$(basename $distributionUrl)
  local dir_name=$(echo "$file_name" | sed 's;-bin.zip;;g' | sed 's;-src.zip;;g' |sed 's;-all.zip;;g')

  if [ ! -d "$base_path" ]; then
    mkdir -p "$base_path"
  else
    # if data already exists, it means we failed to do this before
    # so cleanup last run and try again
    rm -rf $base_path/*
  fi

  # download dist. curl on mac doesn't like the cert provided...
  local zip_path=$(zip_path)
  curl --insecure -L -o "$zip_path/$file_name" "$distributionUrl"

  pushd "$base_path"
    touch "$file_name.lck"
    unzip "$zip_path/$file_name" 1> /dev/null
    touch "$file_name.ok"
  popd
}

is_cached() {
  local file_name=$(basename $distributionUrl)

  [ -e "$(dist_path)/$file_name.ok" ]
}

lib_path() {
  local base_path=$(dist_path)
  local file_name=$(basename $distributionUrl | sed 's;-bin.zip;;g' | sed 's;-src.zip;;g' |sed 's;-all.zip;;g')

  echo "$base_path/$file_name/lib"
}

# Escape application args
save () {
    for i do printf %s\\n "$i" | sed "s/'/'\\\\''/g;1s/^/'/;\$s/\$/' \\\\/" ; done
    echo " "
}

classpath() {
  local dir=$(lib_path)
  local cp=$(ls -1 $dir/*.jar | tr '\n' ':')
  echo "$dir:$cp"
}

# Split up the JVM_OPTS And GRADLE_OPTS values into an array, following the shell quoting and substitution rules
function splitJvmOpts() {
    JVM_OPTS=("$@")
}

main() {
  if ! is_cached; then
    download
  fi

  APP_ARGS=$(save "$@")

  eval splitJvmOpts $DEFAULT_JVM_OPTS $JAVA_OPTS $GRADLE_OPTS
  JVM_OPTS[${#JVM_OPTS[*]}]="-Dorg.gradle.appname=$APP_BASE_NAME"
  exec "$JAVACMD" "${JVM_OPTS[@]}" -cp $(classpath) org.gradle.launcher.GradleMain "$@"
}

main "$@"
