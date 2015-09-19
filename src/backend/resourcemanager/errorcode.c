#include "errorcode.h"
#include "envswitch.h"
#include <string.h>

ErrorDetailData ErrorDetailsPreset[] = {
		{COMM2RM_CLIENT_FAIL_CONN, 				"failing to connect"},
		{COMM2RM_CLIENT_FAIL_SEND, 				"failing to send content"},
		{COMM2RM_CLIENT_FAIL_RECV, 				"failing to receive content"},
		{COMM2RM_CLIENT_WRONG_INPUT, 			"wrong local resource context index"},
		{COMM2RM_CLIENT_FULL_RESOURCECONTEXT, 	"too many resource contexts"},
		{RESQUEMGR_DEADLOCK_DETECTED,			"session resource deadlock is detected"},
		{RESQUEMGR_NOCLUSTER_TIMEOUT,			"no available cluster to run query"},
		{RESQUEMGR_NORESOURCE_TIMEOUT,			"no available resource to run query"},
		{RESQUEMGR_TOO_MANY_FIXED_SEGNUM,		"expecting too many virtual segments"},

		{-1, ""}
};

bool initializedErrorDetails = false;
ErrorDetailData ErrorDetails[ERROR_COUNT_MAX];

const char *getErrorCodeExplain(int errcode)
{
	if ( !initializedErrorDetails )
	{
		for ( int i = 0 ; i < ERROR_COUNT_MAX ; ++i )
		{
			ErrorDetails[i].Code = -1;
			ErrorDetails[i].Message[0] = '\0';
		}

		for ( int i = 0 ; ErrorDetailsPreset[i].Code >= 0 ; ++i )
		{
			ErrorDetails[ErrorDetailsPreset[i].Code].Code = ErrorDetailsPreset[i].Code;
			strncpy(ErrorDetails[ErrorDetailsPreset[i].Code].Message,
					ErrorDetailsPreset[i].Message,
					sizeof(ErrorDetailsPreset[i].Message)-1);
		}

		initializedErrorDetails = true;
	}

	static char message[256];

	if ( errcode < 0 || errcode >= ERROR_COUNT_MAX )
	{
		snprintf(message, sizeof(message), "Invalid error code %d", errcode);
	}
	else if ( ErrorDetails[errcode].Code == -1 )
	{
		snprintf(message, sizeof(message), "Unexplained %d", errcode);
	}
	else {
		return ErrorDetails[errcode].Message;
	}
	return message;
}
