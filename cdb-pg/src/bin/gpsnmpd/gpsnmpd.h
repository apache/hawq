/*-------------------------------------------------------------------------
 * gpsnmpd.h
 *
 *      Global definitions for the main PostgreSQL SNMP Daemon.
 *      This file should be included first in all gpsnmpd source files.
 *
 *      Copyright (c) 2006, PostgreSQL Global Development Group
 *      Author: Joshua Tolley
 *
 *      $Id: pgsnmpd.h,v 1.6 2007/12/12 01:24:06 h-saito Exp $
 *-------------------------------------------------------------------------
 */

#ifndef GPSNMPD_H
#define GPSNMPD_H

#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "postgres_fe.h"
#include "libpq-fe.h"

/* snmp include files expose the autoconf variables about the package, and
 * so does the pg includes. Oops. 
 */ 
#ifdef PACKAGE_BUGREPORT
#undef PACKAGE_BUGREPORT
#endif
#ifdef PACKAGE_NAME
#undef PACKAGE_NAME
#endif
#ifdef PACKAGE_STRING
#undef PACKAGE_STRING
#endif
#ifdef PACKAGE_TARNAME
#undef PACKAGE_TARNAME
#endif
#ifdef PACKAGE_VERSION
#undef PACKAGE_VERSION
#endif


/*
 * Global includes from net-snmp.
 */
#ifndef NETSNMP_USE_INLINE
#define NETSNMP_USE_INLINE 1
#endif
#include <net-snmp/net-snmp-config.h>
#include <net-snmp/net-snmp-includes.h>
#include <net-snmp/agent/net-snmp-agent-includes.h>
#include <net-snmp/agent/mib_modules.h>


extern char *conninfo;
extern PGconn *dbconn;
bool IsAlive(void);

#endif
