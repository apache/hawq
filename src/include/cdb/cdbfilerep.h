/*
 *  cdbfilerep.h
 *
 *
 *  Copyright 2009-2010, Greenplum Inc. All rights reserved.
 *
 */

#ifndef CDBFILEREP_H
#define CDBFILEREP_H

#include "postmaster/primary_mirror_mode.h"

#define FILEREP_UNDEFINED 0xFFFFFFFF

extern FileRepRole_e		fileRepRole;
extern SegmentState_e		segmentState;
extern DataState_e			dataState;


#endif   /* CDBFILEREP_H */

