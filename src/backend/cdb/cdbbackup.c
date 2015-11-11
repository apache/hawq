/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*-------------------------------------------------------------------------
 *
 * cdbbackup.c
 *
 *
 *
 */
#include "postgres.h"
#include <unistd.h>
#include <sys/stat.h>
#include <assert.h>
#include <dirent.h>
#include <sys/wait.h>
#include "regex/regex.h"
#include "gp-libpq-fe.h"
#include "libpq/libpq-be.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbbackup.h"
#include "cdb/cdbtimer.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"

#define EMPTY_STR '\0'

/* general utility copied from cdblink.c */
#define GET_STR(textp) DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))

/* static helper functions */
#ifdef GPDUMP_SUPPORT_IN_HAWQ
static bool createBackupDirectory(char *pszPathName);
static char *findAcceptableBackupFilePathName(char *pszBackupDirectory, char *pszBackupKey, int segid);
static char *formBackupFilePathName(char *pszBackupDirectory, char *pszBackupKey, bool is_compress, bool isPostData);
static char *formStatusFilePathName(char *pszBackupDirectory, char *pszBackupKey, bool bIsBackup);
static char *formThrottleCmd(char *pszBackupFileName, int directIO_read_chunk_mb, bool bIsThrottlingEnabled);
static char *relativeToAbsolute(char *pszRelativeDirectory);
static void testCmdsExist(char *pszCompressionProgram, char *pszBackupProgram);
static char *testProgramExists(char *pszProgramName);
static void validateBackupDirectory(char *pszBackupDirectory);
static char *positionToError(char *source);

static char *compPg = NULL,
		   *bkPg = NULL;
#endif
typedef enum backup_file_type
{
	BFT_BACKUP = 0,
	BFT_BACKUP_STATUS = 1,
	BFT_RESTORE_STATUS = 2
} BackupFileType;


/*
 * gp_backup_launch__( TEXT, TEXT, TEXT, TEXT, TEXT ) returns TEXT
 *
 * Called by gp_dump.c to launch gp_dump_agent on this host.
 */
PG_FUNCTION_INFO_V1(gp_backup_launch__);
Datum
gp_backup_launch__(PG_FUNCTION_ARGS)
{
#ifdef GPDUMP_SUPPORT_IN_HAWQ
	/* Fetch the parameters from the argument list. */
	char	   *pszBackupDirectory = "./";
	char	   *pszBackupKey = "";
	char	   *pszCompressionProgram = "";
	char	   *pszPassThroughParameters = "";
	char	   *pszPassThroughCredentials = "";
	char	   *pszDataOption = "";
	char	   *pszBackupFileName,
		   *pszStatusFileName;
	char       *pszSaveBackupfileName;
	char	   *pszDBName,
	           *pszUserName;
	pid_t	   newpid;
	char	   *pszCmdLine;
	int        port;
	char	   *pszKeyParm;		/* dispatch node */
	int	   segid;
	int	   len;
	int	   instid;			/* dispatch node */
	bool	   is_compress;
	bool	   rsyncable;
	itimers    savetimers;

	char       *pszThrottleCmd;
	char        emptyStr = EMPTY_STR;


	rsyncable = false;
//	instid = AmActiveMaster() ? 1 : 0;	/* dispatch node */
//	segid = GpIdentity.dbid;

	if (!PG_ARGISNULL(0))
	{
		pszBackupDirectory = GET_STR(PG_GETARG_TEXT_P(0));
		if (*pszBackupDirectory == '\0')
			pszBackupDirectory = "./";
	}

	if (!PG_ARGISNULL(1))
		pszBackupKey = GET_STR(PG_GETARG_TEXT_P(1));

	if (!PG_ARGISNULL(2))
		pszCompressionProgram = GET_STR(PG_GETARG_TEXT_P(2));

	if (!PG_ARGISNULL(3))
		pszPassThroughParameters = GET_STR(PG_GETARG_TEXT_P(3));

	if (!PG_ARGISNULL(4))
		pszPassThroughCredentials = GET_STR(PG_GETARG_TEXT_P(4));

	/*
	 * if BackupDirectory is relative, make it absolute based on the directory
	 * where the database resides
	 */
	pszBackupDirectory = relativeToAbsolute(pszBackupDirectory);

	/*
	 * See whether the directory exists.
	 * If not, try to create it.
	 * If so, make sure it's a directory
	 */
	validateBackupDirectory(pszBackupDirectory);

	/*
	 * Validate existence of gp_dump_agent and compression program, if
	 * possible
	 */
	testCmdsExist(pszCompressionProgram, "gp_dump_agent");

	/* are we going to use a compression program? */
	is_compress = (pszCompressionProgram != NULL && *pszCompressionProgram != '\0' ? true : false);

	/* Form backup file path name */
	pszBackupFileName = formBackupFilePathName(pszBackupDirectory, pszBackupKey, is_compress, false);
	pszStatusFileName = formStatusFilePathName(pszBackupDirectory, pszBackupKey, true);

	pszSaveBackupfileName = pszBackupFileName;


	pszDBName = NULL;
	pszUserName = (char *) NULL;
	if (MyProcPort != NULL)
	{
		pszDBName = MyProcPort->database_name;
		pszUserName = MyProcPort->user_name;
	}

	if (pszDBName == NULL)
		pszDBName = "";
	if (pszUserName == NULL)
		pszUserName = "";


	if (strstr(pszPassThroughParameters,"--rsyncable") != NULL)
	{
		rsyncable = true;
		elog(DEBUG1,"--rsyncable found, ptp %s",pszPassThroughParameters);
		/* Remove from gp_dump_agent parameters, because this parameter is for gzip */
		strncpy(strstr(pszPassThroughParameters,"--rsyncable"),"            ",strlen("--rsyncable"));
		elog(DEBUG1,"modified to ptp %s",pszPassThroughParameters);
		/* If a compression program is set, and doesn't already include the parameter */
		if (strlen(pszCompressionProgram) > 1 && strstr(pszCompressionProgram, "--rsyncable") == NULL)
		{
			/* add the parameter */
			/*
			 * You would think we'd add this to pszCompressionProgram, but actually
			 * testCommandExists() moved it to compPg, where we use it later.
			 *
			 * But, much of the code assumes that if pszCompressionProgram is set, we WILL have a compression program,
			 * and the code will fail if we don't, so why we do this is a mystery.
			 */
			char * newComp = palloc(strlen(compPg) + strlen(" --rsyncable") + 1);
			strcpy(newComp,compPg);
			strcat(newComp," --rsyncable");
			compPg = newComp;
			elog(DEBUG1,"compPG %s",compPg);
		}
	}


	/* If a compression program is set, and doesn't already include the parameter */
	if (strlen(pszCompressionProgram) > 1 && strstr(pszCompressionProgram, " -1") == NULL &&
			pszCompressionProgram[0] == 'g')
	{
		/* add the -1 parameter to gzip */
		/*
		 * You would think we'd add this to pszCompressionProgram, but actually
		 * testCommandExists() moved it to compPg, where we use it later.
		 *
		 * But, much of the code assumes that if pszCompressionProgram is set, we WILL have a compression program,
		 * and the code will fail if we don't, so why we do this is a mystery.
		 */
		char * newComp = palloc(strlen(compPg) + strlen(" -1") + 1);
		strcpy(newComp,compPg);
		strcat(newComp," -1");
		compPg = newComp;
		elog(DEBUG1,"new compPG %s",compPg);
	}


	/* Clear process interval timers */
	resetTimers(&savetimers);

	/* Child Process */
	port = PostPortNumber;

	/* Create the --gp-k parameter string */
	pszKeyParm = (char *) palloc(strlen(pszBackupKey) +
								 strlen(pszPassThroughCredentials) +
								 3 + 10 + 10 + 1);
	if (pszKeyParm == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	pszThrottleCmd = &emptyStr;       /* We do this to point to prevent memory leak*/
	if(gp_backup_directIO)
	{
		if (testProgramExists("throttlingD.py") == NULL)
		{
			/*throttlingD.py does not exist*/
			const char *gp_home = getenv("GPHOME");
			if (!gp_home)
				gp_home = "";

			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
							errmsg("throttlingD.py not found in GPHOME\\bin (GPHOME: %s)", gp_home)));
		}

		pszThrottleCmd = formThrottleCmd(pszBackupFileName, gp_backup_directIO_read_chunk_mb, true);
	}
	else
	{
		// when directIO is disabled --> need to pipe the dump to backupFile (No throttling will be done)
		pszThrottleCmd = formThrottleCmd(pszBackupFileName, 0, false);
	}

	/*
	 * dump schema and data for entry db, and data only for segment db.
	 * why dump data for entry db? because that will include statements
	 * to update the sequence tables (last values). As for actual data -
	 * there isn't any, so we will just dump empty COPY statements...
	 */
	pszDataOption = (instid == 1) ? "--pre-data-schema-only" : "-a";

	sprintf(pszKeyParm, "%s_%d_%d_%s", pszBackupKey, instid, segid, pszPassThroughCredentials);

	/*
	 * In the following calc for the number of characters in the
	 * pszCmdLine, I show each piece as the count of the string for that
	 * piece  + 1 for the space after that piece, e.g. -p comes out 2 for
	 * the -p and 1 for the space after the -p	The last 1 is for the
	 * binary zero at the end.
	 */
	len = strlen(bkPg) + 1	/* gp_dump_agent */
		+ 7 + 1				/* --gp-k */
		+ strlen(pszKeyParm) + 1
		+ 7 + 1				/* --gp-d */
		+ strlen(pszBackupDirectory) + 1
		+ 2 + 1				/* -p */
		+ 5 + 1				/* port */
		+ 2 + 1				/* -U */
		+ strlen(pszUserName) + 1	/* pszUserName */
		+ strlen(pszPassThroughParameters) + 1
		+ 23 + 1			/* " " or "--post-data-schema-only" */
		+ strlen("_post_data")
		+ strlen(pszDBName) + 1
		+ 2 + 1				/* 2> */
		+ strlen(pszStatusFileName) + 1
		+ strlen(pszThrottleCmd)
		+ 1;

	/* if user selected a compression program */
	if (pszCompressionProgram[0] != '\0')
	{
		len += 1 + 1		/* | */
		+ strlen(compPg) + 1;
	}

	pszCmdLine = (char *) palloc(len);
	if (pszCmdLine == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	/* if user selected a compression program */
    if (pszCompressionProgram[0] != '\0')
    {
            /* gp_dump_agent + options, pipe into compression program, direct
             * stdout to backup file and stderr to status file */
            sprintf(pszCmdLine, "%s --gp-k %s --gp-d %s -p %d -U %s %s %s %s 2> %s | %s %s",
                            bkPg, pszKeyParm, pszBackupDirectory, port, pszUserName, pszPassThroughParameters,
                            pszDataOption, pszDBName, pszStatusFileName, compPg, pszThrottleCmd);
    }
    else
    {
            sprintf(pszCmdLine, "%s --gp-k %s --gp-d %s -p %d -U %s %s %s %s 2> %s %s",
                            bkPg, pszKeyParm, pszBackupDirectory,
                            port, pszUserName, pszPassThroughParameters, pszDataOption, pszDBName,
                            pszStatusFileName, pszThrottleCmd);
    }

	elog(LOG, "gp_dump_agent command line: %s", pszCmdLine),


	/* Fork off gp_dump_agent	*/
#ifdef _WIN32
	exit(1);
#else
	newpid = fork();
#endif
	if (newpid < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Could not fork a process for backup of database %s", pszDBName)));
	}
	else if (newpid == 0)
	{

	    /* This execs a shell that runs the gp_dump_agent program	*/
		execl("/bin/sh", "sh", "-c", pszCmdLine, NULL);

		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Error in gp_backup_launch - execl of %s with Command Line %s failed",
						"/bin/sh", pszCmdLine)));
		_exit(127);
	}

	/*
	 * If we are the master, we do two calls to gp_dump_agent, one for the pre-schema, one for the post
	 */
	if (instid == 1)
	{
		int stat;
		waitpid(newpid, &stat, 0);

		/* Since we going to re-formated the file names, we need to re-create the command line with the correct length */
		int 	newlen = len -
				strlen(pszBackupFileName) -
				strlen(pszStatusFileName) -
				strlen(pszDataOption) -
				strlen(pszThrottleCmd);

		/* re-format the file names */
		pszBackupFileName = formBackupFilePathName(pszBackupDirectory, pszBackupKey, is_compress, true);
		pszStatusFileName = formStatusFilePathName(pszBackupDirectory, pszBackupKey, true);
		pszDataOption = "--post-data-schema-only" ;

		if(gp_backup_directIO)
		{
			pszThrottleCmd = formThrottleCmd(pszBackupFileName, gp_backup_directIO_read_chunk_mb, true);
		}
		else
		{
			pszThrottleCmd = formThrottleCmd(pszBackupFileName, 0, false);
		}

		/* Delete the old command */
		pfree(pszCmdLine);

		/* Calculate the new length */
		newlen += strlen(pszBackupFileName) +
				strlen(pszStatusFileName) +
				strlen(pszDataOption) +
				strlen(pszThrottleCmd);

		pszCmdLine = (char *) palloc(newlen);
		if (pszCmdLine == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}

		/* if user selected a compression program */
		if (pszCompressionProgram[0] != '\0')
		{
			/* gp_dump_agent + options, pipe into compression program, direct
			 * stdout to backup file and stderr to status file */
			sprintf(pszCmdLine, "%s --gp-k %s --gp-d %s -p %d -U %s %s %s %s 2>> %s | %s %s",
					bkPg, pszKeyParm, pszBackupDirectory, port, pszUserName, pszPassThroughParameters,
					pszDataOption, pszDBName, pszStatusFileName, compPg, pszThrottleCmd);
		}
		else
		{
			sprintf(pszCmdLine, "%s --gp-k %s --gp-d %s -p %d -U %s %s %s %s 2>> %s %s",
					bkPg, pszKeyParm, pszBackupDirectory, 
					port, pszUserName, pszPassThroughParameters, pszDataOption, pszDBName,
					pszStatusFileName, pszThrottleCmd);
		}

		elog(LOG, "gp_dump_agent command line : %s", pszCmdLine),

#ifdef _WIN32
		exit(1);
#else
		newpid = fork();
#endif
		if (newpid < 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					 errmsg("Could not fork a process for backup of database %s", pszDBName)));
		}
		else if (newpid == 0)
		{

			/* This execs a shell that runs the gp_dump_agent program	*/
				execl("/bin/sh", "sh", "-c", pszCmdLine, NULL);

			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					 errmsg("Error in gp_backup_launch - execl of %s with Command Line %s failed",
							"/bin/sh", pszCmdLine)));
			_exit(127);
		}

	}

	/* Restore process interval timers */
	restoreTimers(&savetimers);

	assert(pszSaveBackupfileName != NULL && pszSaveBackupfileName[0] != '\0');

	return DirectFunctionCall1(textin, CStringGetDatum(pszSaveBackupfileName));
#else
	PG_RETURN_NULL();
#endif
}


/*
 * gp_restore_launch__( TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, INT, bool ) returns TEXT
 *
 * Called by gp_restore.c to launch gp_restore_agent on this host.
 */
PG_FUNCTION_INFO_V1(gp_restore_launch__);
Datum
gp_restore_launch__(PG_FUNCTION_ARGS)
{
#ifdef GPDUMP_SUPPORT_IN_HAWQ
	/* Fetch the parameters from the argument list. */
	char	   *pszBackupDirectory = "./";
	char	   *pszBackupKey = "";
	char	   *pszCompressionProgram = "";
	char	   *pszPassThroughParameters = "";
	char	   *pszPassThroughCredentials = "";
	char	   *pszPassThroughTargetInfo = "";
	bool		bOnErrorStop = false;
	int			instid;			/* dispatch node */
	int			segid;
	int			target_dbid = 0; /* keep compiler quiet. value will get replaced */
	char	   *pszOnErrorStop;
	char	   *pszBackupFileName;
	char	   *pszStatusFileName;
	struct stat info;
	char	   *pszDBName;
	char	   *pszUserName;
	pid_t		newpid;
	char	   *pszCmdLine;
	int			port;
	char	   *pszKeyParm;
	int			len,
				len_name;
	bool		is_decompress;
	bool		is_file_compressed; /* is the dump file ending with '.gz'*/
	bool		postDataSchemaOnly;
	itimers 	savetimers;

	postDataSchemaOnly = false;
//	instid = (GpIdentity.segindex == -1) ? 1 : 0;		/* dispatch node */
//	segid = GpIdentity.dbid;

	if (!PG_ARGISNULL(0))
	{
		pszBackupDirectory = GET_STR(PG_GETARG_TEXT_P(0));
		if (*pszBackupDirectory == '\0')
			pszBackupDirectory = "./";
	}

	if (!PG_ARGISNULL(1))
		pszBackupKey = GET_STR(PG_GETARG_TEXT_P(1));

	if (!PG_ARGISNULL(2))
		pszCompressionProgram = GET_STR(PG_GETARG_TEXT_P(2));

	if (!PG_ARGISNULL(3))
		pszPassThroughParameters = GET_STR(PG_GETARG_TEXT_P(3));

	if (!PG_ARGISNULL(4))
		pszPassThroughCredentials = GET_STR(PG_GETARG_TEXT_P(4));

	if (!PG_ARGISNULL(5))
		pszPassThroughTargetInfo = GET_STR(PG_GETARG_TEXT_P(5));

	if (!PG_ARGISNULL(6))
		target_dbid = PG_GETARG_INT32(6);

	if (!PG_ARGISNULL(7))
		bOnErrorStop = PG_GETARG_BOOL(7);

	pszOnErrorStop = bOnErrorStop ? "--gp-e" : "";

	/*
	 * if BackupDirectory is relative, make it absolute based on the directory
	 * where the database resides
	 */
	pszBackupDirectory = relativeToAbsolute(pszBackupDirectory);

	/* Validate existence of compression program and gp_restore_agent program */
	testCmdsExist(pszCompressionProgram, "gp_restore_agent");

	/* are we going to use a decompression program? */
	is_decompress = (pszCompressionProgram != NULL && *pszCompressionProgram != '\0' ? true : false);

	/* Post data pass? */
	postDataSchemaOnly = strstr(pszPassThroughParameters, "--post-data-schema-only") != NULL;

	/* Form backup file path name */
	pszBackupFileName = formBackupFilePathName(pszBackupDirectory, pszBackupKey, is_decompress, postDataSchemaOnly);
	pszStatusFileName = formStatusFilePathName(pszBackupDirectory, pszBackupKey, false);

	if (0 != stat(pszBackupFileName, &info))
	{
		pszBackupFileName = findAcceptableBackupFilePathName(pszBackupDirectory, pszBackupKey, segid);
		if (pszBackupFileName == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("Attempt to restore dbid %d failed. No acceptable dump file name found "
					" in directory %s with backup key %s and source dbid key %d", target_dbid, 
					pszBackupDirectory, pszBackupKey, segid)));

		/*
		 * If --gp-c is on make sure our file name has a .gz suffix.
		 * If no decompression requested make sure the file *doesn't* have
		 * a .gz suffix. This fixes the problem with forgetting to use
		 * the compression flags and gp_restore completing silently with
		 * no errors (hard to reproduce, but this check doesn't hurt)
		 */
		len_name = strlen(pszBackupFileName);
		is_file_compressed = (strcmp(pszBackupFileName + (len_name - 3), ".gz") == 0 ? true : false);

		if (is_file_compressed && !is_decompress)
			ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("dump file appears to be compressed. Pass a decompression "
						  "option to gp_restore to decompress it on the fly")));

		if (!is_file_compressed && is_decompress)
			ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				errmsg("a decompression cmd line option is used but the dump "
						  "file does not appear to be compressed.")));
	}

	len_name = strlen(pszBackupFileName);

	pszDBName = NULL;
	pszUserName = NULL;
	if (MyProcPort != NULL)
	{
		pszDBName = MyProcPort->database_name;
		pszUserName = MyProcPort->user_name;
	}

	if (pszDBName == NULL)
		pszDBName = "";
	if (pszUserName == NULL)
		pszUserName = "";

	/* Clear process interval timers */
	resetTimers(&savetimers);

	/* Fork off gp_restore_agent  */
#ifdef _WIN32
	exit(1);
#else
	newpid = fork();
#endif
	if (newpid < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Could not fork a process for backup of database %s", pszDBName)));
	}
	else if (newpid == 0)
	{
		/* Child Process */
		port = PostPortNumber;

		/* Create the --gp-k parameter string */
		pszKeyParm = (char *) palloc(strlen(pszBackupKey) +
									 strlen(pszPassThroughCredentials) +
									 3 + 10 + 10 + 1);
		if (pszKeyParm == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}

		sprintf(pszKeyParm, "%s_%d_%d_%s", pszBackupKey, instid, segid, pszPassThroughCredentials);

	/*
	 * In the following calc for the number of characters in the
	 * pszCmdLine, I show each piece as the count of the string for that
	 * piece  + 1 for the space after that piece, e.g. -p comes out 2 for
	 * the -p and 1 for the space after the -p	The last 1 is for the
	 * binary zero at the end.
	 */
	len = strlen(bkPg) + 1	/* gp_restore_agent */
		+ 7 + 1				/* --gp-k */
		+ strlen(pszKeyParm) + 1
		+ 7 + 1				/* --gp-d */
		+ strlen(pszBackupDirectory) + 1
		+ 7 + 1				/* --gp-e */
		+ 2 + 1				/* -p */
		+ 5 + 1				/* port */
		+ 2 + 1				/* -U */
		+ strlen(pszUserName) + 1	/* pszUserName */
		+ strlen(pszPassThroughParameters) + 1
		+ strlen(pszPassThroughTargetInfo) + 1
		+ 2 + 1				/* -d */
		+ strlen(pszDBName) + 1
		+ 1 + 1				/* > */
		+ strlen(pszStatusFileName)
		+ 4 + 1				/* 2>&1 */
		+ 1;

	/*
	 * if compression was requested with --gp-c
	 */
	if (is_decompress)
	{
		len += 4 + strlen(compPg) +
			+strlen(pszBackupFileName);

		len += 8; /* pass along "--gp-c" to tell restore file is compressed */

		pszCmdLine = (char *) palloc(len);
		if (pszCmdLine == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}

		/*
		 * de-compress backupfile and pipe into stdin of gp_restore_agent along with its
		 * options. Redirect both stdout and stderr into the status file.
		 */
		sprintf(pszCmdLine, "%s --gp-c %s --gp-k %s --gp-d %s %s -p %d -U %s %s %s -d %s %s %s %s 2>&2",
				bkPg, compPg, pszKeyParm, pszBackupDirectory, pszOnErrorStop, port, pszUserName,
				pszPassThroughParameters, pszPassThroughTargetInfo, pszDBName, pszBackupFileName,
				postDataSchemaOnly ? "2>>" : "2>", pszStatusFileName);
	}
	else
	{
		len += strlen(pszBackupFileName) + 1;

		pszCmdLine = (char *) palloc(len);
		if (pszCmdLine == NULL)
		{
			ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		}

		/* Format gp_restore_agent with options, and Redirect both stdout and stderr into the status file */
		sprintf(pszCmdLine, "%s --gp-k %s --gp-d %s %s -p %d -U %s %s %s -d %s %s %s %s 2>&2",
				bkPg, pszKeyParm, pszBackupDirectory, pszOnErrorStop, port, pszUserName, pszPassThroughParameters,
				pszPassThroughTargetInfo, pszDBName, pszBackupFileName,
				postDataSchemaOnly ? "2>>" : "2>", pszStatusFileName);
	}


		elog(LOG, "gp_restore_agent command line: %s", pszCmdLine),

		/* This execs a shell that runs the gp_restore_agent program  */
			execl("/bin/sh", "sh", "-c", pszCmdLine, NULL);

		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Error in gp_restore_launch - execl of %s with Command Line %s failed",
						"/bin/sh", pszCmdLine)));
		_exit(127);
	}


	/* Restore process interval timers */
	restoreTimers(&savetimers);

	assert(pszBackupFileName != NULL && pszBackupFileName[0] != '\0');

	return DirectFunctionCall1(textin, CStringGetDatum(pszBackupFileName));
#else
	PG_RETURN_NULL();
#endif
}

/*
 * gp_read_backup_file__( TEXT, TEXT, int ) returns TEXT
 *
 */
PG_FUNCTION_INFO_V1(gp_read_backup_file__);
Datum
gp_read_backup_file__(PG_FUNCTION_ARGS)
{
#ifdef GPDUMP_SUPPORT_IN_HAWQ
	/* Fetch the parameters from the argument list. */
	char	   *pszBackupDirectory = "./";
	char	   *pszBackupKey = "";
	int			fileType = 0;
	char	   *pszFileName;
	struct stat info;
	char	   *pszFullStatus = NULL;
	FILE	   *f;

	if (!PG_ARGISNULL(0))
	{
		pszBackupDirectory = GET_STR(PG_GETARG_TEXT_P(0));
		if (*pszBackupDirectory == '\0')
			pszBackupDirectory = "./";
	}

	if (!PG_ARGISNULL(1))
		pszBackupKey = GET_STR(PG_GETARG_TEXT_P(1));

	if (!PG_ARGISNULL(2))
		fileType = PG_GETARG_INT32(2);

	/*
	 * if BackupDirectory is relative, make it absolute based on the directory
	 * where the database resides
	 */
	pszBackupDirectory = relativeToAbsolute(pszBackupDirectory);

	/* Form backup file path name */
	pszFileName = NULL;
	switch (fileType)
	{
		case BFT_BACKUP:
			/*pszFileName = formBackupFilePathName(pszBackupDirectory, pszBackupKey);*/
			/*we can't support this without making a change to this function arguments, which
			  will require an initdb, which we want to avoid. This is due to the fact that in
			  this call to formBackupFilePathName we don't know if the file is compressed or
			  not and formBackupFilePathName will not know if to create a file with a compression
			  suffix or not */
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					 errmsg("Only status files are currently supported in gp_read_backup_file")));
			break;
		case BFT_BACKUP_STATUS:
		case BFT_RESTORE_STATUS:
			pszFileName = formStatusFilePathName(pszBackupDirectory, pszBackupKey, (fileType == BFT_BACKUP_STATUS));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					 errmsg("Invalid filetype %d passed to gp_read_backup_file", fileType)));
			break;
	}

	/* Make sure pszFileName exists */
	if (0 != stat(pszFileName, &info))
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Backup File %s Type %d could not be be found", pszFileName, fileType)));
	}

	/* Read file */
	if (info.st_size > INT_MAX)
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Backup File %s Type %d too large to read", pszFileName, fileType)));
	}

	/* Allocate enough memory for entire file, and read it in. */
	pszFullStatus = (char *) palloc(info.st_size + 1);
	if (pszFullStatus == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	f = fopen(pszFileName, "r");
	if (f == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Backup File %s Type %d cannot be opened", pszFileName, fileType)));
	}

	if (info.st_size != fread(pszFullStatus, 1, info.st_size, f))
	{
		fclose(f);
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Error reading Backup File %s Type %d", pszFileName, fileType)));
	}

	fclose(f);
	f = NULL;
	pszFullStatus[info.st_size] = '\0';
	
	return DirectFunctionCall1(textin, CStringGetDatum(positionToError(pszFullStatus)));
#else
	PG_RETURN_NULL();
#endif
}

/*
 * gp_write_backup_file__( TEXT, TEXT, TEXT ) returns TEXT
 *
 */

/* NOT IN USE ANYMORE! */

/*
 * gp_write_backup_file__( TEXT, TEXT, TEXT ) returns TEXT
 *
 */
/*
 * Greenplum TODO: This function assumes the file is not compressed.
 * This is not always the case. Therefore it should be used carefully
 * by the caller, until we decide to make the change and pass in an
 * isCompressed boolean, which unfortunately will require an initdb for
 * the change to take effect.
 */
PG_FUNCTION_INFO_V1(gp_write_backup_file__);
Datum
gp_write_backup_file__(PG_FUNCTION_ARGS)
{
#ifdef GPDUMP_SUPPORT_IN_HAWQ
	/* Fetch the parameters from the argument list. */
	char	   *pszBackupDirectory = "./";
	char	   *pszBackupKey = "";
	char	   *pszBackup = "";
	char	   *pszFileName;
	FILE	   *f;
	int			nBytes;

	if (!PG_ARGISNULL(0))
	{
		pszBackupDirectory = GET_STR(PG_GETARG_TEXT_P(0));
		if (*pszBackupDirectory == '\0')
			pszBackupDirectory = "./";
	}

	if (!PG_ARGISNULL(1))
		pszBackupKey = GET_STR(PG_GETARG_TEXT_P(1));

	if (!PG_ARGISNULL(2))
		pszBackup = GET_STR(PG_GETARG_TEXT_P(2));

	/*
	 * if BackupDirectory is relative, make it absolute based on the directory
	 * where the database resides
	 */
	pszBackupDirectory = relativeToAbsolute(pszBackupDirectory);

	/*
	 * See whether the directory exists.
	 * If not, try to create it.
	 * If so, make sure it's a directory
	 */
	validateBackupDirectory(pszBackupDirectory);

	/* Form backup file path name */
	pszFileName = formBackupFilePathName(pszBackupDirectory, pszBackupKey, false, false);

	/* Write file */
	f = fopen(pszFileName, "w");
	if (f == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Backup File %s cannot be opened", pszFileName)));
	}

	nBytes = strlen(pszBackup);
	if (nBytes != fwrite(pszBackup, 1, nBytes, f))
	{
		fclose(f);
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("Error writing Backup File %s", pszFileName)));
	}

	fclose(f);
	f = NULL;

	assert(pszFileName != NULL && pszFileName[0] != '\0');

	return DirectFunctionCall1(textin, CStringGetDatum(pszFileName));
#else
	PG_RETURN_NULL();
#endif
}

#ifdef GPDUMP_SUPPORT_IN_HAWQ
/*
 * createBackupDirectory( char* pszPathName ) return bool
 *
 * This will create a directory if it doesn't exist.
 *
 * This routine first attempts to create the directory using the full path (in case the
 * mkdir function is able to create the full directory).  If this mkdir fails with EEXIST,
 * filesystem object is checked to ensure it is a directory and fails with ENOTDIR if it
 * is not.
 *
 * If the "full" mkdir fails with a code other than ENOENT, this routine fails.  For the
 * ENOENT code, the routine attempts to create the directory tree from the top, one level
 * at a time, failing if unable to create a directory with a code other than EEXIST and
 * the EEXIST level is not a directory.  Being tolerant of EEXIST failures allows for
 * multiple segments to attempt to create the directory simultaneously.
 */
bool
createBackupDirectory(char *pszPathName)
{
	int			rc;
	struct stat info;
	char	   *pSlash;

	/* Can we make it in its entirety?	If so, return */
	if (0 == mkdir(pszPathName, S_IRWXU))
		return true;

	if (errno == ENOENT)
	{
		/* Try to create each level of the hierarchy that doesn't already exist */
		pSlash = pszPathName + 1;

		while (pSlash != NULL && *pSlash != '\0')
		{
			/* Find next subdirectory delimiter */
			pSlash = strchr(pSlash, '/');
			if (pSlash == NULL)
			{
				/*
				 * If no more slashes we're at the last level. Attempt to
				 * create it; if it fails for any reason other than EEXIST,
				 * fail the call.
				 */
				rc = mkdir(pszPathName, S_IRWXU);
				if (rc != 0 && errno != EEXIST)
				{
					return false;
				}
			}
			else
			{
				/* Temporarily end the string at the / we just found. */
				*pSlash = '\0';

				/* Attempt to create the level; return code checked below. */
				rc = mkdir(pszPathName, S_IRWXU);

				/*
				 * If failed and the directory level does not exist, fail the call.
				 * Is it possible for a directory level to exist and the mkdir() call
				 * fail with a code such as ENOSYS (and possibly others) instead of
				 * EEXIST.  So, if mkdir() fails, the directory level's existence is
				 * checked.  If the level does't exist or isn't a directory, the
				 * call is failed.
				 */
				if (rc != 0)
				{
					struct stat statbuf;
					int errsave = errno;	/* Save mkdir error code over stat() call. */

					if ( stat(pszPathName, &statbuf) < 0 || !S_ISDIR(statbuf.st_mode) )
					{
						errno = errsave;		/* Restore mkdir error code. */
						*pSlash = '/';			/* Put back the slash we overwrote above. */
						return false;
					}
				}

				/* Put back the slash we overwrote above. */
				*pSlash = '/';

				/*
				 * At this point, this level exists -- either because it was created
				 * or it already exists.  If it already exists, it *could* be a
				 * non-directory.  This will be caught when attempting to create the
				 * next level.
				 */

				/* Advance past the slash. */
				pSlash++;
			}
		}
	}
	else if (errno != EEXIST)
	{
		return false;
	}

	/* At this point, the path has been created or otherwise exists.  Ensure it's a directory. */
	if (0 != stat(pszPathName, &info))
	{
		/* An unexpected failure obtaining the file info. errno is set. */
		return false;
	}
	else if (!S_ISDIR(info.st_mode))
	{
		/* The pathname exists but is not a directory! */
		errno = ENOTDIR;
		return false;
	}

	return true;
}

/* findAcceptableBackupFilePathName( char* pszBackupDirectory, char* pszBackupKey ) returns char*
 *
 * This function takes the directory and timestamp key and finds an existing file that matches
 * the naming convention for a backup file with the proper key and segid, but any instid.
 * In case it was backed up from a redundant instance of the same segment.
 */
char *
findAcceptableBackupFilePathName(char *pszBackupDirectory, char *pszBackupKey, int segid)
{
	/* Make sure that pszBackupDirectory exists and is in fact a directory. */
	struct stat buf;
	struct dirent *dirp = NULL;
	DIR		   *dp = NULL;
	char	   *pszBackupFilePathName = NULL;
	static regex_t rFindFileName;
	static bool bFirstTime = true;
	char	   *pszRegex;
	char	   *pszFileName;
	char	   *pszSep;
	struct stat info;

	if (bFirstTime)
	{
		int			wmasklen,
						masklen;
			pg_wchar   *mask;
		pszRegex = (char *) palloc(34 + strlen(pszBackupKey) + 1);
		if (pszRegex == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}

		/* dump file can start with gp_ and also mpp_ for 2.3 backward compatibility */
		sprintf(pszRegex, "^(gp|mpp)_dump_[0-9]+_%d_%s(.gz)?$", segid, pszBackupKey);

		masklen = strlen(pszRegex);
		mask = (pg_wchar *) palloc((masklen + 1) * sizeof(pg_wchar));
		wmasklen = pg_mb2wchar_with_len(pszRegex, mask, masklen);

		if (0 != pg_regcomp(&rFindFileName, mask, wmasklen, REG_EXTENDED))
		{
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					 errmsg("Could not compile regular expression for backup filename matching")));
		}

		bFirstTime = false;
	}

	if (lstat(pszBackupDirectory, &buf) < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		 errmsg("Backup Directory %s does not exist.", pszBackupDirectory)));
	}

	if (S_ISDIR(buf.st_mode) == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Backup Location %s is not a directory.", pszBackupDirectory)));
	}

	dp = opendir(pszBackupDirectory);
	if (dp == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Backup Directory %s cannot be opened for enumerating files.", pszBackupDirectory)));
	}

	while (NULL != (dirp = readdir(dp)))
	{
		pg_wchar   *data;
		size_t		data_len;
		int			newfile_len;
		pszFileName = dirp->d_name;

		/* Convert data string to wide characters */
		newfile_len = strlen(pszFileName);
		data = (pg_wchar *) palloc((newfile_len + 1) * sizeof(pg_wchar));
		data_len = pg_mb2wchar_with_len(pszFileName, data, newfile_len);

		if (0 == pg_regexec(&rFindFileName, data, data_len, 0, NULL, 0, NULL, 0))
		{
			pszSep = "/";
			if (strlen(pszBackupDirectory) >= 1 && pszBackupDirectory[strlen(pszBackupDirectory) - 1] == '/')
				pszSep = "";

			pszBackupFilePathName = (char *) palloc(strlen(pszBackupDirectory) + strlen(pszFileName) + strlen(pszSep) + 1);
			sprintf(pszBackupFilePathName, "%s%s%s", pszBackupDirectory, pszSep, pszFileName);

			/* Make sure that this is a regular file */
			if (0 == stat(pszBackupFilePathName, &info) && S_ISREG(info.st_mode))
			{
				break;
			}
		}
	}

	closedir(dp);

	return pszBackupFilePathName;
}
/* formBackupFilePathName( char* pszBackupDirectory, char* pszBackupKey ) returns char*
 *
 * This function takes the directory and timestamp key and creates
 * the path and filename of the backup output file
 * based on the naming convention for this.
 */
char *
formBackupFilePathName(char *pszBackupDirectory, char *pszBackupKey, bool is_compress, bool isPostData)
{
#ifndef PATH_MAX
#define PATH_MAX 1024
#endif
	/* First form the prefix */
	char		szFileNamePrefix[1 + PATH_MAX];
	int			instid;			/* dispatch node */
	int			segid;
	int			len;
	char	   *pszBackupFileName;

//	instid = (GpIdentity.segindex == -1) ? 1 : 0;		/* dispatch node */
//	segid = GpIdentity.dbid;

	sprintf(szFileNamePrefix, "gp_dump_%d_%d_", instid, segid);

	/* Now add up the length of the pieces */
	len = strlen(pszBackupDirectory);
	assert(len >= 1);
	if (pszBackupDirectory[strlen(pszBackupDirectory) - 1] != '/')
		len++;

	len += strlen(szFileNamePrefix);
	len += strlen(pszBackupKey);

	/* if gzip/gunzip is used, the suffix is gz */
	if (is_compress)
		len += strlen(".gz");

	if (isPostData)
		len += strlen("_post_data");

	if (len > PATH_MAX)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Backup FileName based on path %s and key %s too long", pszBackupDirectory, pszBackupKey)));
	}

	pszBackupFileName = (char *) palloc(sizeof(char) * (1 + len));
	if (pszBackupFileName == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	strcpy(pszBackupFileName, pszBackupDirectory);
	if (pszBackupDirectory[strlen(pszBackupDirectory) - 1] != '/')
		strcat(pszBackupFileName, "/");

	strcat(pszBackupFileName, szFileNamePrefix);
	strcat(pszBackupFileName, pszBackupKey);
	
	if (isPostData)
		strcat(pszBackupFileName, "_post_data");

	/* if gzip/gunzip is used, the suffix is gz */
	if (is_compress)
		strcat(pszBackupFileName, ".gz");

	return pszBackupFileName;
}

/* formStatusFilePathName( char* pszBackupDirectory, char* pszBackupKey, bool bIsBackup ) returns char*
 *
 * This function takes the directory and timestamp key and IsBackup flag and creates
 * the path and filename of the backup or restore status file
 * based on the naming convention for this.
 */

char *
formStatusFilePathName(char *pszBackupDirectory, char *pszBackupKey, bool bIsBackup)
{
	/* First form the prefix */
	char		szFileNamePrefix[1 + PATH_MAX];
	int			instid;			/* dispatch node */
	int			segid;
	int			len;
	char	   *pszFileName;

//	instid = (GpIdentity.segindex == -1) ? 1 : 0;		/* dispatch node */
//	segid = GpIdentity.dbid;

	sprintf(szFileNamePrefix, "gp_%s_status_%d_%d_", (bIsBackup ? "dump" : "restore"),
			instid, segid);

	/* Now add up the length of the pieces */
	len = strlen(pszBackupDirectory);
	assert(len >= 1);
	if (pszBackupDirectory[strlen(pszBackupDirectory) - 1] != '/')
		len++;

	len += strlen(szFileNamePrefix);
	len += strlen(pszBackupKey);

	if (len > PATH_MAX)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Status FileName based on path %s and key %s too long", pszBackupDirectory, pszBackupKey)));
	}

	pszFileName = (char *) palloc(sizeof(char) * (1 + len));
	if (pszFileName == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	strcpy(pszFileName, pszBackupDirectory);
	if (pszBackupDirectory[strlen(pszBackupDirectory) - 1] != '/')
		strcat(pszFileName, "/");

	strcat(pszFileName, szFileNamePrefix);
	strcat(pszFileName, pszBackupKey);

	return pszFileName;
}

/* formThrottleCmd(char *pszBackupFileName, int directIO_read_chunk_mb, bool bIsThrottlingEnabled) returns char*
 *
 * This function takes the backup file name and creates
 * the throttlin command
 */
char *
formThrottleCmd(char *pszBackupFileName, int directIO_read_chunk_mb, bool bIsThrottlingEnabled)
{
	int 	throttleCMDLen = 0;
	char	*pszThrottleCmd;

	if(bIsThrottlingEnabled)
	{
		throttleCMDLen = strlen("| throttlingD.py ")
				+ 3 + 1     /* gp_backup_directIO_read_chunk_mb */
				+ strlen(pszBackupFileName)
				+ 1;

		/* Create the throttlingD.py string */
		pszThrottleCmd = (char *)palloc(throttleCMDLen);
		if (pszThrottleCmd == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
		}
		sprintf(pszThrottleCmd, "| throttlingD.py %d %s",directIO_read_chunk_mb ,pszBackupFileName);
	}
	else
	{
		// when directIO is disabled --> need to pipe the dump to backupFile (No throttling will be done)
		throttleCMDLen = 2 + 1     /* > */
				+ strlen(pszBackupFileName)
				+ 1;

		/* Create the empty throttling string */
		pszThrottleCmd = (char *)palloc(throttleCMDLen);

		if (pszThrottleCmd == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
		}

		sprintf(pszThrottleCmd, " > %s",pszBackupFileName);
	}

	return pszThrottleCmd;
}

/*
 * relativeToAbsolute( char* pszRelativeDirectory ) returns char*
 *
 * This will turn a relative path into an absolute one, based off of the Data Directory
 * for this database.
 *
 * If the path isn't absolute, it is prefixed with DataDir/.
 */
char *
relativeToAbsolute(char *pszRelativeDirectory)
{
	char	   *pszAbsolutePath;

	if (pszRelativeDirectory[0] == '/')
		return pszRelativeDirectory;

	pszAbsolutePath = (char *) palloc(strlen(DataDir) + 1 + strlen(pszRelativeDirectory) + 1);
	if (pszAbsolutePath == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	sprintf(pszAbsolutePath, "%s/%s", DataDir, pszRelativeDirectory);

	return pszAbsolutePath;
}

/* testCmdsExist( char* pszCompressionProgram ) returns void
 *
 * This function tests whether pszBackupProgram and the pszCompressionProgram
 * (if non-empty) are in the path.	If not, it calls ereport.
 */
void
testCmdsExist(char *pszCompressionProgram, char *pszBackupProgram)
{
	/* Does pszBackupProgram exist? */
	if (pszBackupProgram != NULL &&
		*pszBackupProgram != '\0' &&
		(bkPg = testProgramExists(pszBackupProgram)) == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("%s not found in PGPATH or PATH (PGPATH: %s | PATH:  %s)",
						pszBackupProgram, getenv("PGPATH"), getenv("PATH")),
				 errhint("Restart the server and try again")));
	}

	/* Does compression utility exist? */
	if (pszCompressionProgram != NULL &&
		*pszCompressionProgram != '\0' &&
		(compPg = testProgramExists(pszCompressionProgram)) == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("%s (compression utility) not found in in PGPATH or "
						"PATH (PGPATH: %s | PATH:  %s)",
						pszCompressionProgram, getenv("PGPATH"), getenv("PATH"))));
	}
}

/* testProgramExists(char* pszProgramName) returns bool
 * This runs a shell with which pszProgramName > tempfile
 * piping the output to a temp file.
 * If the file is empty, then pszProgramName didn't exist
 * on the path.
 */
char *
testProgramExists(char *pszProgramName)
{
	char	   *pszProgramNameLocal;
	char	   *p;

	/* Now see what the length of the file is  - 0 means not there */
	struct stat buf;
	char	   *pszEnvPath;
	char	   *pszPath;
	char	   *pColon;
	char	   *pColonNext;
	char	   *pszTestPath;

	if (pszProgramName == NULL || *pszProgramName == '\0')
		return NULL;

	/*
	 * The pszProgramName might have command line arguments, so we need to
	 * stop at the first whitespace
	 */
	pszProgramNameLocal = palloc(strlen(pszProgramName) + 1);
	if (pszProgramNameLocal == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	strcpy(pszProgramNameLocal, pszProgramName);

	p = pszProgramNameLocal;
	while (*p != '\0')
	{
		if (*p == ' ' || *p == '\t' || *p == '\n')
			break;

		p++;
	}

	*p = '\0';

	pszEnvPath = getenv("PGPATH");
	if (pszEnvPath != NULL)
	{
		pszTestPath = (char *) palloc(strlen(pszEnvPath) + 1 + strlen(pszProgramNameLocal) + 1);
		if (pszTestPath == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}

		sprintf(pszTestPath, "%s/%s", pszEnvPath, pszProgramNameLocal);
		if (stat(pszTestPath, &buf) >= 0)
			return pszTestPath;
	}
	pszEnvPath = getenv("PATH");
	if (pszEnvPath == NULL)
		return NULL;
	pszPath = (char *) palloc(strlen(pszEnvPath) + 1);
	if (pszPath == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	strcpy(pszPath, pszEnvPath);
	/* Try to create each level of the hierarchy that doesn't already exist */
	pColon = pszPath;

	while (pColon != NULL && *pColon != '\0')
	{
		/* Find next delimiter */
		pColonNext = strchr(pColon, ':');

		if (pColonNext == pColon)
		{
			pColon++;
			continue;
		}

		if (pColonNext == NULL)
		{
			/* See whether pszProgramName exists in subdirectory pColon */
			pszTestPath = (char *) palloc(strlen(pColon) + 1 + strlen(pszProgramNameLocal) + 1);
			if (pszTestPath == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			sprintf(pszTestPath, "%s/%s", pColon, pszProgramNameLocal);
			if (stat(pszTestPath, &buf) >= 0)
				return pszTestPath;
			else
				return NULL;
		}

		/* Temporarily end the string at the : we just found. */
		*pColonNext = '\0';

		/* See whether pszProgramName exists in subdirectory pColon */
		pszTestPath = (char *) palloc(strlen(pColon) + 1 + strlen(pszProgramNameLocal) + 1);
		if (pszTestPath == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}

		sprintf(pszTestPath, "%s/%s", pColon, pszProgramNameLocal);
		if (stat(pszTestPath, &buf) >= 0)
		{
			*pColonNext = ':';
			return pszTestPath;
		}

		/* Put back the colon we overwrote above. */
		*pColonNext = ':';

		/* Advance past the colon. */
		pColon = pColonNext + 1;

		/* free */
		pfree(pszTestPath);
	}

	return NULL;
}
/* validateBackupDirectory( char* pszBackupDirectory ) returns void
 * This sees whether the backupDirectory exists
 * If so, it makes sure its a directory and not a file.
 * If not, it tries to create it.  If it cannot it calls ereport
 */
void
validateBackupDirectory(char *pszBackupDirectory)
{
	struct stat info;

	if (stat(pszBackupDirectory, &info) < 0)
	{
		elog(DEBUG1, "Backup Directory %s does not exist", pszBackupDirectory);

		/* Attempt to create it. */
		if (!createBackupDirectory(pszBackupDirectory))
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create backup directory \"%s\": %m",
							pszBackupDirectory)));
		}
		else
		{
			elog(DEBUG1, "Successfully created Backup Directory %s", pszBackupDirectory);
		}
	}
	else
	{
		if (!S_ISDIR(info.st_mode))
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("BackupDirectory %s exists but is not a directory.", pszBackupDirectory)));
		}
	}
}
/* 
 * positionToError - given a char buffer, position to the first error msg in it.
 * 
 * we are only interested in ERRORs. therefore, search for the first ERROR
 * and return the rest of the status file starting from that point. if no error
 * found, return a "no error found" string. We are only interested in occurrences
 * of "ERROR:" (from the backend), or [ERROR] (from dump/restore log). others,
 * like "ON_ERROR_STOP" should be ignored.
 */
static char *positionToError(char *source)
{
	char*	sourceCopy = source;
	char*	firstErr = NULL;
	char*	defaultNoErr = "No extra error information available. Please examine status file.";

	while(true)
	{
		firstErr = strstr((const char*)sourceCopy, "ERROR");
		
		if (!firstErr)
		{
			break;
		}
		else
		{
			/* found "ERROR". only done if it's "ERROR:" or "[ERROR]" */
			if (firstErr[5] == ']' || firstErr[5] == ':')
			{
				break; /* done */
			}
			else
			{
				sourceCopy = ++firstErr; /* start from stopping point */
				firstErr = NULL;
			}
		}
	}
	
	if (!firstErr)
	{
		/* no [ERROR] found */
		firstErr = defaultNoErr;
	}
	else
	{
		/* found [ERROR]. go back to beginning of the line */
		char *p;
		
		for (p = firstErr ; p > source ; p--)
		{
			if (*p == '\n')
			{
				p++; /* skip the LF */
				break;
			}
		}
		
		firstErr = p;
	}
	
	return firstErr;
}

#endif
