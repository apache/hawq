dnl
dnl tomcat.m4: Locates the Apache Tomcat/6.0.x and its scripts and jars. 
dnl

# PGAC_CATALINA_HOME
# ------------------
AC_DEFUN([PGAC_CATALINA_HOME],
[
  AC_MSG_CHECKING([CATALINA_HOME])

  dnl Use $CATALINA_HOME if specifed, otherwise look for java in the likely places.
  dnl /usr/local/Cellar/tomcat@6/6.0.45/libexec 
  dnl /usr/lib/bigtop-tomcat
  if test -x "${CATALINA_HOME}/bin/catalina.sh"; then
    TOMCAT="${CATALINA_HOME}"
  elif test -x "/usr/local/Cellar/tomcat@6/6.0.45/libexec/bin/catalina.sh"; then
    TOMCAT="/usr/local/Cellar/tomcat@6/6.0.45/libexec/"
  elif test -x "/usr/lib/bigtop-tomcat/bin/catalina.sh"; then
    TOMCAT="/usr/lib/bigtop-tomcat/"
  else
    AC_MSG_RESULT(not found)
    AC_MSG_ERROR([CATALINA_HOME not found])
  fi
  CATALINA_HOME="$TOMCAT"
  
  AC_MSG_RESULT(${CATALINA_HOME})
  AC_SUBST(CATALINA_HOME)
])

# PGAC_PATH_TOMCAT
# ----------------
AC_DEFUN([PGAC_PATH_TOMCAT],
[
  CATALINA=none
  SETCLASSPATH=none
  
  AC_REQUIRE([PGAC_CATALINA_HOME])
  AC_PATH_PROGS(CATALINA,catalina.sh,,${CATALINA_HOME}/bin)
  AC_PATH_PROGS(SETCLASSPATH,setclasspath.sh,,${CATALINA_HOME}/bin)
])

# PGAC_CHECK_TOMCAT_EMBED_SETUP
# ----------------------------
AC_DEFUN([PGAC_CHECK_TOMCAT_EMBED_SETUP],
[
  AC_REQUIRE([PGAC_PATH_TOMCAT])

  # Check the current tomcat version
  AC_MSG_CHECKING([for tomcat version])
  version=`${CATALINA} version 2>&1 | grep "Server version" | cut -d '/' -f2`
  AC_MSG_RESULT("$version")

  if echo "$version" | sed ['s/[^0-9]/ /g'] | $AWK '{ if ([$]1 != 6) exit 0; else exit 1;}'
  then
    AC_MSG_ERROR([The installed version of tomcat is not compatiable to use with ranger-plugin.
                  Apache Tomcat/6.0.x is required, but this is $version])
  fi

])
