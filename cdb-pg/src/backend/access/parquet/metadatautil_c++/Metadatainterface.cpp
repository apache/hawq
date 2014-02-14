
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

int readFileMetadata(/*IN*/uint8_t *buf, /*IN*/uint32_t len,
/*IN*/int compact, ParquetMetadata_4C** ppfileMetadata) {
	MetadataUtil util;
	*ppfileMetadata = (struct ParquetMetadata_4C*)palloc0(sizeof(struct ParquetMetadata_4C));
	bool compactBool = (compact == 1) ? true : false;
	int iret = util.readFileMetadata(buf, len, compactBool, *ppfileMetadata);
	return iret;
}

int writeFileMetadata(/*IN*/uint8_t **buf,
		/*IN*/uint32_t *len,
		ParquetMetadata_4C* ppfileMetadata) {
	int iret = MetadataUtil::writeFileMetadata(buf, len, ppfileMetadata);
	return iret;
}

int readPageMetadata(/*IN*/uint8_t *buf, /*IN*/uint32_t *len,
/*IN*/int compact, struct PageMetadata_4C** ppageMetdata){
	MetadataUtil util;
	*ppageMetdata = (struct PageMetadata_4C*)palloc0(sizeof(struct PageMetadata_4C));
	bool compactBool = (compact == 1) ? true : false;
	int iret = util.readPageMetadata(buf, len, compactBool, *ppageMetdata);
	return iret;
}

int writePageMetadata(/*IN*/uint8_t **buf,
		/*IN*/uint32_t *len,
		struct PageMetadata_4C* ppageMetadata){
	int iret = MetadataUtil::writePageMetadata(buf, len, ppageMetadata);
	return iret;
}

int writeColumnChunkMetadata(/*IN*/uint8_t **buf,
		/*IN*/uint32_t *len,
		struct ColumnChunkMetadata_4C* blockMetadata){

	int iret = MetadataUtil::writeColumnChunkMetadata(buf, len, blockMetadata);
	return iret;
}
}
