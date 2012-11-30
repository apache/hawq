/*-------------------------------------------------------------------------
 *
 * execworkfile.c
 *    Management of temporary work files used by the executor nodes.
 *
 * Copyright (c) 2010. Greenplum Inc.
 *
 * WorkFiles provide a general interface to different implementations of
 * temporary files used by the exuecutor nodes. Currently, this is only
 * used in HashJoin, but could be extended to other executor nodes that
 * might require spilling data to disk during execution, such as HashAgg,
 * Material, Sort, and etc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/buffile.h"
#include "storage/bfz.h"
#include "executor/execWorkfile.h"

/*
 * ExecWorkFile_Create
 *    create a new work file with the specified name, the file type,
 * and the compression type.
 *
 * If this fails, NULL is returned.
 */
ExecWorkFile *
ExecWorkFile_Create(const char *fileName,
					ExecWorkFileType fileType,
					int compressType)
{
	ExecWorkFile *workfile = NULL;
	void *file = NULL;

	switch(fileType)
	{
		case BUFFILE:
			file = (void *)BufFileCreateTemp(fileName, false);
			break;
		case BFZ:
			file = (void *)bfz_create(fileName, compressType);
			break;
		default:
			ereport(LOG,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("invalid work file type: %d", fileType)));
			Assert(false);
	}

	workfile = palloc0(sizeof(ExecWorkFile));

	workfile->fileType = fileType;
	workfile->compressType = compressType;
	workfile->file = file;
	
	return workfile;
}

/*
 * ExecWorkFile_Write
 *    write the given data from the end of the last write position.
 *
 * This function returns true if the write succeeds. Otherwise, return false.
 */
bool
ExecWorkFile_Write(ExecWorkFile *workfile,
				   void *data,
				   uint64 size)
{
	Assert(workfile != NULL);
	uint64 bytes;

	if (data == NULL || size == 0)
		return false;
	
	switch(workfile->fileType)
	{
		case BUFFILE:
			bytes = BufFileWrite((BufFile *)workfile->file, data, size);
			if (bytes != size)
				return false;
			break;
		case BFZ:
			bfz_append((bfz_t *)workfile->file, data, size);
			break;
		default:
			insist_log(false, "invalid work file type: %d", workfile->fileType);
	}
	
	return true;
}

/*
 * ExecWorkFile_Read
 *    read the data with specified size to the given buffer.
 *
 * The given buffer should contain at least the space specified by 
 * 'size'.
 *
 * If the read succeeds, this function returns the number of bytes
 * that are read. Otherwise, returns 0.
 */
uint64
ExecWorkFile_Read(ExecWorkFile *workfile,
				  void *data,
				  uint64 size)
{
	Assert(workfile != NULL);
	uint64 bytes = 0;
	
	switch(workfile->fileType)
	{
		case BUFFILE:
			bytes = BufFileRead((BufFile *)workfile->file, data, size);
			break;
			
		case BFZ:
			bytes = bfz_scan_next((bfz_t *)workfile->file, data, size);
			break;
		default:
			insist_log(false, "invalid work file type: %d", workfile->fileType);
	}
	
	return bytes;
}

/*
 * ExecWorkFile_Rewind
 *    rewind the pointer position to the beginning of the file.
 *
 * This function returns true if this succeeds. Otherwise, return false.
 */
bool
ExecWorkFile_Rewind(ExecWorkFile *workfile)
{
	Assert(workfile != NULL);

	long ret = 0;
	switch(workfile->fileType)
	{
		case BUFFILE:
			ret = BufFileSeek((BufFile *)workfile->file, 0, 0L, SEEK_SET);
			/* BufFileSeek returns 0 if everything went OK */
			return 0 == ret;
		case BFZ:
			bfz_append_end((bfz_t *)workfile->file);
			bfz_scan_begin((bfz_t *)workfile->file);
			break;
		default:
			insist_log(false, "invalid work file type: %d", workfile->fileType);
	}

	return true;
}

/*
 * ExecWorkFile_Tell64
 *    return the value of the current file position indicator.
 */
uint64
ExecWorkFile_Tell64(ExecWorkFile *workfile)
{
	Assert(workfile != NULL);
	uint64 bytes = 0;
	
	switch(workfile->fileType)
	{
		case BUFFILE:
			bytes = BufFileTell64((BufFile *)workfile->file);
			break;
			
		case BFZ:
			bytes = bfz_totalbytes((bfz_t *)workfile->file);
			break;
		default:
			insist_log(false, "invalid work file type: %d", workfile->fileType);
	}

	return bytes;
}

/*
 * ExecWorkFile_Close
 *    close the work file, and release the space.
 */
void
ExecWorkFile_Close(ExecWorkFile *workfile)
{
	Assert(workfile != NULL);

	switch(workfile->fileType)
	{
		case BUFFILE:
			BufFileClose((BufFile *)workfile->file);
			break;
			
		case BFZ:
			bfz_close((bfz_t *)workfile->file, true);
			break;
		default:
			insist_log(false, "invalid work file type: %d", workfile->fileType);
	}

	pfree(workfile);
}
