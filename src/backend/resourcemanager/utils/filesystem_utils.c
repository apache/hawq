#include "utils/filesystem_utils.h"
#include <sys/stat.h>
#include <unistd.h>

FileConfiguration createFileConfiguration(MCTYPE context, char *filename)
{
	FileConfiguration result = NULL;

	uint32_t filenamelen = strlen(filename);
	result = rm_palloc0(context,
						offsetof(FileConfigurationData, FileName) +
						__SIZE_ALIGN64(filenamelen+1));

	memcpy(result->FileName, filename, filenamelen+1);
	result->FileNameLength 	= filenamelen;
	result->InstanceSize	= offsetof(FileConfigurationData, FileName) +
							  __SIZE_ALIGN64(filenamelen+1);
	return result;
}

int updateFileConfigurationAsLocalFile(FileConfiguration fileconfig, bool testio)
{
	/* Check if the file exists*/
	fileconfig->Exist    = access(fileconfig->FileName, F_OK) == 0 ?
							      STAT_TRUE :
							      STAT_FALSE;

	fileconfig->Readable = access(fileconfig->FileName, R_OK) == 0 ?
								  STAT_TRUE :
								  STAT_FALSE;

	fileconfig->Writable = access(fileconfig->FileName, W_OK) == 0 ?
								  STAT_TRUE :
								  STAT_FALSE;

	/* Get file stat to have more details. */
	struct stat filestat;
	int statres = stat(fileconfig->FileName, &filestat);
	if ( statres != 0 ) {
		/* We dont think this directory can be accessed. */
		fileconfig->AccessMask 		 = 0;
		fileconfig->Exist	   		 = STAT_FALSE;
		fileconfig->Readable   		 = STAT_UNSET;
		fileconfig->Writable   		 = STAT_UNSET;
		fileconfig->Reserved		 = 0;
		fileconfig->isDirectory		 = STAT_UNSET;
		fileconfig->ReadBytesPerSec  = -1.0;
		fileconfig->WriteBytesPerSec = -1.0;
		return UTIL_FILESYSTEM_FAIL_GET_FILESTAT;
	}

	fileconfig->isDirectory = S_ISDIR(filestat.st_mode) ?
							  STAT_TRUE :
							  STAT_FALSE;

	fileconfig->AccessMask = filestat.st_mode;

	return FUNC_RETURN_OK;
}
