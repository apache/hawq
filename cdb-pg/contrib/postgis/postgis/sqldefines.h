#ifndef _LWPGIS_DEFINES
#define _LWPGIS_DEFINES

/*
 * Define just the version numbers; otherwise we get some strange substitutions in postgis.sql.in
 */
#define POSTGIS_PGSQL_VERSION 90
#define POSTGIS_GEOS_VERSION 32
#define POSTGIS_PROJ_VERSION 46
#define POSTGIS_LIB_VERSION  1.4.2
/*
 * Define the build date and the version number
 * (these substitiutions are done with extra quotes sinces CPP
 * won't substitute within apostrophes)
 */
#define _POSTGIS_SQL_SELECT_POSTGIS_VERSION 'SELECT ''1.4 USE_GEOS=1 USE_PROJ=1 USE_STATS=1''::text AS version'
#define _POSTGIS_SQL_SELECT_POSTGIS_BUILD_DATE 'SELECT ''2010-03-11 14:50:27''::text AS version'
#define _POSTGIS_SQL_SELECT_POSTGIS_SCRIPTS_VERSION 'SELECT ''1.4.2''::text AS version'

#endif /* _LWPGIS_DEFINES */
