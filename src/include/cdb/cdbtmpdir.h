#ifndef CDBTMPDIR_H
#define CDBTMPDIR_H

void getLocalTmpDirFromMasterConfig(int session_id);
void getLocalTmpDirFromSegmentConfig(int session_id, int command_id, int qeidx);

#endif
