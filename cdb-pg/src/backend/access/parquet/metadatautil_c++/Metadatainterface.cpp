
#include <iostream>
#include <stdint.h>
#include <boost/shared_ptr.hpp>
#include <boost/math_fwd.hpp>
#include <thrift/protocol/TBinaryProtocol.h>
#include <fcntl.h>
#include <fstream>
#include <stdio.h>
#include <bitset>
#include "MetadataUtil.h"
using namespace hawq;
using namespace std;
using namespace boost;

extern "C" {
#include "postgres.h"
#include "utils/palloc.h"

/*
 * Read buffer to get page metadata
 */
int
readPageMetadata(
		uint8_t 				*buf,
		uint32_t				*len,
		int 					compact,
		struct PageMetadata_4C	**ppageMetdata)
{
	*ppageMetdata = (struct PageMetadata_4C*)palloc0(sizeof(struct PageMetadata_4C));
	bool compactBool = (compact == 1) ? true : false;
	int iret = MetadataUtil::readPageMetadata(buf, len, compactBool, *ppageMetdata);
	return iret;
}

/*
 * Write hawq page metadata to buffer
 */
int
writePageMetadata(
		uint8_t					**buf,
		uint32_t				*len,
		struct PageMetadata_4C	*ppageMetadata)
{
	int iret = MetadataUtil::writePageMetadata(buf, len, ppageMetadata);
	return iret;
}

/*
 * Write hawq column chunk metadata to buffer
 */
int
writeColumnChunkMetadata(
		uint8_t							**buf,
		uint32_t						*len,
		struct ColumnChunkMetadata_4C	*blockMetadata)
{
	int iret = MetadataUtil::writeColumnChunkMetadata(buf, len, blockMetadata);
	return iret;
}
}
