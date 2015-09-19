/*-------------------------------------------------------------------------
 *
 * cdb_dump_util.h
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDB_DUMP_UTIL_H
#define CDB_DUMP_UTIL_H

#include <regex.h>
#include "cdb_seginst.h"

#define CDB_BACKUP_KEY_LEN 14


/* --------------------------------------------------------------------------------------------------
 * This needs to be the same as that in cdbbackup.h in src/backend/cdb/cdbbackup.c
 */
typedef enum backup_file_type
{
	BFT_BACKUP = 0,
	BFT_BACKUP_STATUS = 1,
	BFT_RESTORE_STATUS = 2
} BackupFileType;

/* --------------------------------------------------------------------------------------------------
 * Structure for holding the command line options for both backup and restore
 */
typedef struct input_options
{
	char	   *pszDBName;
	char	   *pszPGHost;
	char	   *pszPGPort;
	char	   *pszUserName;
	char	   *pszBackupDirectory;
	char	   *pszReportDirectory;
	char	   *pszCompressionProgram;
	char	   *pszPassThroughParms;
	char	   *pszCmdLineParms;
	char	   *pszKey;
	char	   *pszMasterDBName;
	char	   *pszRawDumpSet;
	ActorSet	actors;
	BackupLoc	backupLocation;
	bool		bOnErrorStop;
}	InputOptions;

/* issues either a listen or notify command on connection pConn */
extern void DoCancelNotifyListen(PGconn *pConn, bool bListen,
					 const char *pszCDBDumpKey,
					 int CDBInstID,
					 int CDBSegID,
					 int target_db_id,
					 const char *pszSuffix);

/* frees data allocated inside an InputOptions struct */
extern void FreeInputOptions(InputOptions * pInputOpts);

/* Generates a 14 digit timestamp key */
extern char *GenerateTimestampKey(void);

/* gets an int from a regex match */
extern int	GetMatchInt(regmatch_t *pMatch, char *pszInput);

/* gets an stringt from a regex match */
extern char *GetMatchString(regmatch_t *pMatch, char *pszInput);

/* creates a formatted time string with the current time */
extern char *GetTimeNow(char *szTimeNow);

/* creates a connection where the password may be prompted for */
extern PGconn *GetMasterConnection(const char *progName, const char *pszDBName, const char *pszPGHost,
					const char *pszPGPort, const char *pszUserName,
					int reqPwd, int ignoreVersion, bool bDispatch);

/* creates a connection based on SegmentInstance* parameter */
extern PGconn *MakeDBConnection(const SegmentDatabase *pSegDB, bool bDispatch);

/* returns a formatted char * */
extern char *MakeString(const char *fmt,...);

/* breaks the input parameter associated with --cdb-k into its components for cdb_dump and cdb_restore*/
extern bool ParseCDBDumpInfo(const char *progName, char *pszCDBDumpInfo, char **ppCDBDumpKey, int *pCDBInstID, int *pCDBSegID, char **ppCDBPassThroughCredentials);

/* reads the contents out of the appropriate file on the database server */
extern char *ReadBackendBackupFile(PGconn *pConn, const char *pszBackupDirectory, const char *pszKey, BackupFileType fileType, const char *progName);

/* returns strdup if not NULL, NULL otherwise */
extern char *Safe_strdup(const char *s);

/* returns the string if not null, otherwise the default */
extern const char *StringNotNull(const char *pszInput, const char *pszDefault);

/* writes a formatted message to stderr */
extern void mpp_err_msg(const char *loglevel, const char *prog, const char *fmt,...);

/* writes a formatted message to stderr and caches it in a static var for later use */
extern void mpp_err_msg_cache(const char *loglevel, const char *prog, const char *fmt,...);

/* writes a formatted message to stdout */
extern void mpp_msg(const char *loglevel, const char *prog, const char *fmt,...);

/* writes the contents to the appropriate file on the database server */
/*extern char* WriteBackendBackupFile( PGconn* pConn, const char* pszBackupDirectory, const char* pszKey, const char* pszBackupScript );*/
/* writes a status message to a status file with naming convention based on instid, segid, and Key */
/* extern bool WriteStatusToFile( const char* pszMessage, const char* pszDirectory, const char* pszKey, int instid, int segid, bool bIsBackup ); */

extern char *get_early_error(void);

/* Base64 Encoding and Decoding Routines */
/* Base64 Data is assumed to be in a NULL terminated string */
/* Data is just assumed to be an array of chars, with a length */
extern char *DataToBase64(char *pszIn, unsigned int InLen);
extern char *Base64ToData(char *pszIn, unsigned int *pOutLen);
extern char *nextToken(register char **stringp, register const char *delim);
extern int	parseDbidSet(int *dbidset, char *dump_set);

#endif   /* CDB_DUMP_UTIL_H */
