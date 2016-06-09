/**********************************************************************
 * =============================================================================
 * Portions of this file modified by EMC Corporation, 2011

 * The only modifications made to the open source code were minor bug fixes.
 * This product may be distributed with open source code, licensed to you in
 * accordance with the applicable open source license.  If you would like a copy
 * of any such source code, EMC will provide a copy of the source code that is
 * required to be made available in accordance with the applicable open source
 * license.  EMC may charge reasonable shipping and handling charges for such
 * distribution.  Please direct requests in writing to
 * EMC Legal, 176 South St., Hopkinton, MA 01748, ATTN: Open Source Program Office.
 * =============================================================================
 **********************************************************************/

#ifndef PG_LIBSRID_H
#define PG_LIBSRID_H

char * getProj4StringStatic(int srid);
int    is_srid_planar(int srid);
char * getSRSbySRIDbyRule(int srid, bool short_crs, char *buf);
int    getSRIDbySRSbyRule(const char* srs);

#endif /* PG_LIBSRID_H*/
