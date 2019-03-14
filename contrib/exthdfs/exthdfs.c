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

#include "postgres.h"

#include "common.h"
#include "access/extprotocol.h"
#include "cdb/cdbdatalocality.h"
#include "storage/fd.h"
#include "storage/filesystem.h"
#include "utils/uri.h"




PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(hdfsprotocol_blocklocation);
PG_FUNCTION_INFO_V1(hdfsprotocol_validate);

Datum hdfsprotocol_blocklocation(PG_FUNCTION_ARGS);
Datum hdfsprotocol_validate(PG_FUNCTION_ARGS);

Datum hdfsprotocol_blocklocation(PG_FUNCTION_ARGS)
{

	// Build the result instance
	int nsize = 0;
	int numOfBlock = 0;
	ExtProtocolBlockLocationData *bldata =
		palloc0(sizeof(ExtProtocolBlockLocationData));
	if (bldata == NULL)
	{
		elog(ERROR, "hdfsprotocol_blocklocation : "
                    "cannot allocate due to no memory");
	}
	bldata->type = T_ExtProtocolBlockLocationData;
	fcinfo->resultinfo = bldata;

	ExtProtocolValidatorData *pvalidator_data = (ExtProtocolValidatorData *)
												(fcinfo->context);


	 // Parse URI of the first location, we expect all locations uses the same
	 // name node server. This is checked in validation function.

	char *first_uri_str = (char *)strVal(lfirst(list_head(pvalidator_data->url_list)));
	Uri *uri = ParseExternalTableUri(first_uri_str);

	elog(DEBUG3, "hdfsprotocol_blocklocation : "
				 "extracted HDFS name node address %s:%d",
				 uri->hostname, uri->port);

	// Create file system instance
	hdfsFS fs = hdfsConnect(uri->hostname, uri->port);
	if (fs == NULL)
	{
		elog(ERROR, "hdfsprotocol_blocklocation : "
					"failed to create HDFS instance to connect to %s:%d",
					uri->hostname, uri->port);
	}

	// Clean up uri instance as we don't need it any longer
	FreeExternalTableUri(uri);

	// Check all locations to get files to fetch location.
	ListCell *lc = NULL;
	foreach(lc, pvalidator_data->url_list)
	{
		// Parse current location URI.
		char *url = (char *)strVal(lfirst(lc));
		Uri *uri = ParseExternalTableUri(url);
		if (uri == NULL)
		{
			elog(ERROR, "hdfsprotocol_blocklocation : "
						"invalid URI encountered %s", url);
		}

		 //
		 // NOTICE: We temporarily support only directories as locations. We plan
		 //        to extend the logic to specifying single file as one location
		 //         very soon.


		// get files contained in the path.
		hdfsFileInfo *fiarray = hdfsListDirectory(fs, uri->path,&nsize);
		if (fiarray == NULL)
		{
			elog(ERROR, "hdfsprotocol_blocklocation : "
						"failed to get files of path %s",
						uri->path);
		}

		int i = 0 ;
		// Call block location api to get data location for each file
		for (i = 0 ; i < nsize ; i++)
		{
			hdfsFileInfo *fi = &fiarray[i];

			// break condition of this for loop
			if (fi == NULL) {break;}

			// Build file name full path.
			const char *fname = fi->mName;
			char *fullpath = palloc0(                // slash
									 strlen(fname) +      // name
									 1);                  // \0
			sprintf(fullpath, "%s", fname);

			elog(DEBUG3, "hdfsprotocol_blocklocation : "
						 "built full path file %s", fullpath);

			// Get file full length.
			int64_t len = fi->mSize;

			elog(DEBUG3, "hdfsprotocol_blocklocation : "
					     "got file %s length " INT64_FORMAT,
					     fullpath, len);

			if (len == 0) {
				pfree(fullpath);
				continue;
			}

			// Get block location data for this file
			BlockLocation *bla = hdfsGetFileBlockLocations(fs, fullpath, 0, len,&numOfBlock);
			if (bla == NULL)
			{
				elog(ERROR, "hdfsprotocol_blocklocation : "
							"failed to get block location of path %s. "
							"It is reported generally due to HDFS service errors or "
							"another session's ongoing writing.",
							fullpath);
			}

			// Add file full path and its block number as result.
			blocklocation_file *blf = palloc0(sizeof(blocklocation_file));
			blf->file_uri = pstrdup(fullpath);
			blf->block_num = numOfBlock;
			blf->locations = palloc0(sizeof(BlockLocation) * blf->block_num);

			elog(DEBUG3, "hdfsprotocol_blocklocation : file %s has %d blocks",
					  	 fullpath, blf->block_num);

			// We don't need it any longer
			pfree(fullpath);
			int bidx = 0;
			// Add block information as a list.
			for (bidx = 0 ; bidx < blf->block_num ; bidx++)
			{
				BlockLocation *blo = &bla[bidx];
				BlockLocation *bl = &(blf->locations[bidx]);
				bl->numOfNodes = blo->numOfNodes;
				bl->hosts = (char **)palloc0(sizeof(char *) * bl->numOfNodes);
				bl->names = (char **)palloc0(sizeof(char *) * bl->numOfNodes);
				bl->topologyPaths = (char **)palloc0(sizeof(char *) * bl->numOfNodes);
				bl->offset = blo->offset;
				bl->length = blo->length;
				bl->corrupt = blo->corrupt;

				int nidx = 0 ;
				for (nidx = 0 ; nidx < bl->numOfNodes ; nidx++)
				{
					bl->hosts[nidx] = pstrdup(*blo[nidx].hosts);
					bl->names[nidx] = pstrdup(*blo[nidx].names);
					bl->topologyPaths[nidx] =pstrdup(*blo[nidx].topologyPaths);
				}
			}

			bldata->files = lappend(bldata->files, (void *)(blf));

			// Clean up block location instances created by the lib.
			hdfsFreeFileBlockLocations(bla,numOfBlock);
		}

		// Clean up URI instance in loop as we don't need it any longer
		FreeExternalTableUri(uri);

		// Clean up file info array created by the lib for this location.
		hdfsFreeFileInfo(fiarray,nsize);
	}

	// destroy fs instance
	hdfsDisconnect(fs);

	PG_RETURN_VOID();

}

Datum hdfsprotocol_validate(PG_FUNCTION_ARGS)
{
	elog(DEBUG3, "hdfsprotocol_validate() begin");

	/* Check which action should perform. */
	ExtProtocolValidatorData *pvalidator_data =
       (ExtProtocolValidatorData *)(fcinfo->context);

	if (pvalidator_data->forceCreateDir)
		Assert(pvalidator_data->url_list && pvalidator_data->url_list->length == 1);

	if (pvalidator_data->direction == EXT_VALIDATE_WRITE)
	{
		/* accept only one directory location */
		if (list_length(pvalidator_data->url_list) != 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("hdfsprotocol_validate : "
							"only one location url is supported for writable external hdfs")));
		}
	}

	/* Go through first round to get formatter type */
	bool isCsv = false;
	bool isText = false;
	bool isOrc = false;
	ListCell *optcell = NULL;
	foreach(optcell, pvalidator_data->format_opts)
	{
		DefElem *de = (DefElem *)lfirst(optcell);
		if (strcasecmp(de->defname, "formatter") == 0)
		{
			char *val = strVal(de->arg);
			if (strcasecmp(val, "csv") == 0)
			{
				isCsv = true;
			}
			else if (strcasecmp(val, "text") == 0)
			{
				isText = true;
			}
			else if (strcasecmp(val, "orc") == 0)
			{
				isOrc = true;
			}
		}
	}

	if (!isCsv && !isText && !isOrc)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("hdfsprotocol_validate : "
						"only 'csv', 'text' and 'orc' formatter is supported for external hdfs")));
	}
	Assert(isCsv || isText || isOrc);

	/* Validate formatter options */
	foreach(optcell, pvalidator_data->format_opts)
	{
		DefElem *de = (DefElem *)lfirst(optcell);
		if (strcasecmp(de->defname, "delimiter") == 0)
		{
			char *val = strVal(de->arg);
			/* Validation 1. User can not specify 'OFF' in delimiter */
			if (strcasecmp(val, "off") == 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("hdfsprotocol_validate : "
								"'off' value of 'delimiter' option is not supported")));
			}
			/* Validation 2. Can specify multibytes characters */
			if (strlen(val) < 1)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("hdfsprotocol_validate : "
										"'delimiter' option accepts multibytes characters")));
			}
		}

		if (strcasecmp(de->defname, "escape") == 0)
		{
			char *val = strVal(de->arg);
			/* Validation 3. User can not specify 'OFF' in delimiter */
			if (strcasecmp(val, "off") == 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("hdfsprotocol_validate : "
								"'off' value of 'escape' option is not supported")));
			}
			/* Validation 4. Can only specify one character */
			if (strlen(val) != 1)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("hdfsprotocol_validate : "
										"'escape' option accepts single character")));
			}
		}

		if (strcasecmp(de->defname, "newline") == 0)
		{
			char *val = strVal(de->arg);
			/* Validation 5. only accept 'lf', 'cr', 'crlf' */
			if (strcasecmp(val, "lf") != 0 &&
				strcasecmp(val, "cr") != 0 &&
				strcasecmp(val, "crlf") != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("hdfsprotocol_validate : "
								"the value of 'newline' option can only be "
								"'lf', 'cr' or 'crlf'")));
			}
		}

		if (strcasecmp(de->defname, "quote") == 0)
		{
			/* This is allowed only for csv mode formatter */
			if (!isCsv)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("hdfsprotocol_validate : "
										"'quote' option is only available in 'csv' formatter")));
			}

			char *val = strVal(de->arg);
			/* Validation 5. Can only specify one character */
			if (strlen(val) != 1)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("hdfsprotocol_validate : "
										"'quote' option accepts single character")));
			}
		}

		if (strcasecmp(de->defname, "force_notnull") == 0)
		{
			/* This is allowed only for csv mode formatter */
			if (!isCsv)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("hdfsprotocol_validate : "
										"'force_notnull' option is only available in 'csv' formatter")));
			}
		}

		if (strcasecmp(de->defname, "force_quote") == 0)
		{
			/* This is allowed only for csv mode formatter */
			if (!isCsv)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("hdfsprotocol_validate : "
										"'force_quote' option is only available in 'csv' formatter")));
			}
		}
	}

	/* All urls should
	 * 1) have the same protocol name 'hdfs',
	 * 2) the same hdfs namenode server address
	 */
	/* Check all locations to get files to fetch location. */
	char *nnaddr = NULL;
	int nnport = -1;
	ListCell *lc = NULL;
	foreach(lc, pvalidator_data->url_list)
	{
		/* Parse current location URI. */
		char *url = (char *)strVal(lfirst(lc));
		Uri *uri = ParseExternalTableUri(url);
		if (uri == NULL)
		{
			elog(ERROR, "hdfsprotocol_validate : "
						"invalid URI encountered %s", url);
		}

		if (uri->protocol != URI_HDFS)
		{
			elog(ERROR, "hdfsprotocol_validate : "
						"invalid URI protocol encountered in %s, "
						"hdfs:// protocol is required",
						url);
		}

		if (nnaddr == NULL)
		{
			nnaddr = pstrdup(uri->hostname);
			nnport = uri->port;
		}
		else
		{
			if (strcmp(nnaddr, uri->hostname) != 0)
			{
				elog(ERROR, "hdfsprotocol_validate : "
							"different name server addresses are detected, "
							"both %s and %s are found",
							nnaddr, uri->hostname);
			}
			if (nnport != uri->port)
			{
				elog(ERROR, "hdfsprotocol_validate : "
							"different name server ports are detected, "
							"both %d and %d are found",
							nnport, uri->port);
			}
		}

		/* SHOULD ADD LOGIC HERE TO CREATE UNEXISTING PATH */
		if (pvalidator_data->forceCreateDir) {

		  elog(LOG, "hdfs_validator() forced creating dir");

		  /* Create file system instance */
		  	hdfsFS fs = hdfsConnect(uri->hostname, uri->port);
			if (fs == NULL)
			{
				elog(ERROR, "hdfsprotocol_validate : "
							"failed to create HDFS instance to connect to %s:%d",
							uri->hostname, uri->port);
			}

			if (hdfsExists(fs, uri->path) == -1)
				elog(ERROR, "hdfsprotocol_validate : "
						"Location \"%s\" is not exist",
						uri->path);

		 /* destroy fs instance */
			hdfsDisconnect(fs);
		}

		/* Clean up temporarily created instances */
		FreeExternalTableUri(uri);
		if (nnaddr != NULL)
		{
			pfree(nnaddr);
		}
	}

	elog(LOG, "passed validating hdfs protocol options");

	/**************************************************************************
	 * This is a bad implementation that we check formatter options here. Should
	 * be moved to call formatter specific validation UDFs.
	 **************************************************************************/

	PG_RETURN_VOID();
}

