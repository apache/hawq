# config/maven.m4

# PGAC_PATH_MAVEN
# ---------------
# Look for maven, set the output variable MAVEN to its path if found.
# Reject versions before 3.0.0 (they have bugs or capacity limits).

AC_DEFUN([PGAC_PATH_MAVEN],
[# Let the user override the search
if test -z "$MAVEN"; then
  AC_PATH_PROGS(MAVEN, mvn)
fi

if test "$MAVEN"; then
  pgac_maven_version=`$MAVEN --version 2>/dev/null | sed q | $AWK '{print [$]1" "[$]2" "[$]3;}'`
  AC_MSG_NOTICE([using $pgac_maven_version])
  if echo "$pgac_maven_version" | sed ['s/[^0-9]/ /g'] | $AWK '{ if ([$]1 < 3) exit 0; else exit 1;}'
  then
    AC_MSG_WARN([
*** The installed version of Maven, $MAVEN, is too old to use with HAWQ.
*** Maven version 3.0.0 or later is required, but this is $pgac_maven_version.])
    MAVEN=""
  fi
fi

if test -z "$MAVEN"; then
  AC_MSG_WARN([
*** Without Maven you will not be able to build HAWQ with Mapreduce
*** InputFormat/OutputFormat support.])
fi
# We don't need AC_SUBST(MAVEN) because AC_PATH_PROG did it.
AC_SUBST(MAVEN)
])# PGAC_PATH_MAVEN


