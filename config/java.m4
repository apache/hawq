dnl
dnl java.m4: Locates the JDK and its include files and libraries.
dnl

# PGAC_JAVA_HOME
# --------------
AC_DEFUN([PGAC_JAVA_HOME],
[ 
  AC_MSG_CHECKING([JAVA_HOME])

  dnl Use $JAVA_HOME if specified, otherwise look for java in the likely places.
  dnl Note that Darwin's JVM layout is pretty weird.  JDK 1.6 apears to be the
  dnl default for Snow Leopard, but JDK 1.5 is for Leopard.
  dnl See http://developer.apple.com/qa/qa2001/qa1170.html
  if test -x "${JAVA_HOME}/bin/java"; then
    JDK="${JAVA_HOME}"
  elif test -x "/System/Library/Frameworks/JavaVM.framework/Versions/1.6/Home/bin/java"; then
    JDK="/System/Library/Frameworks/JavaVM.framework/Versions/1.6/Home"
  elif test -x "/usr/java/bin/java"; then
    JDK="/usr/java"
  else
    AC_MSG_RESULT(not found)
    AC_MSG_ERROR([JAVA_HOME not found])
  fi
  JAVA_HOME="$JDK"

  AC_MSG_RESULT(${JAVA_HOME})
  AC_SUBST(JAVA_HOME)
])

# PGAC_PATH_JAVA
# --------------
AC_DEFUN([PGAC_PATH_JAVA],
[ 
  JAVA=none
  JAVAC=none
  JAVAH=none
  JAR=none
  JAVADOC=none

  AC_REQUIRE([PGAC_JAVA_HOME])  
  AC_PATH_PROGS(JAVA,java,,${JAVA_HOME}/bin)
  AC_PATH_PROGS(JAVAC,javac,,${JAVA_HOME}/bin)
  AC_PATH_PROGS(JAVAH,javah,,${JAVA_HOME}/bin)
  AC_PATH_PROGS(JAR,jar,,${JAVA_HOME}/bin)
  AC_PATH_PROGS(JAVADOC,javadoc,,${JAVA_HOME}/bin)
])


# PGAC_CHECK_JAVA_EMBED_SETUP
# --------------
AC_DEFUN(PGAC_CHECK_JAVA_EMBED_SETUP,
[
  AC_REQUIRE([PGAC_PATH_JAVA])

  # Check the current java version
  AC_MSG_CHECKING([for Java version])
  version=`${JAVA} -version 2>&1 | grep version`
  AC_MSG_RESULT("$version")

  # Setup compilation FLAGS to support compiling for JDK 1.6
  AC_MSG_CHECKING([for JAVAC_FLAGS])
  JAVAC_FLAGS="-target 1.6 -source 1.6"
  AC_MSG_RESULT(${JAVAC_FLAGS})
  AC_SUBST(JAVAC_FLAGS)

  # Test Java FLAGS
  AC_MSG_CHECKING([for Java JDK 1.6 support])
  version_check=`${JAVAC} ${JAVAC_FLAGS} 2>&1 | grep invalid`
  if test "$version_check" != ""; then
    AC_MSG_RESULT(no)
    AC_MSG_ERROR("$version_check")
  else
    AC_MSG_RESULT(yes)
  fi
])

