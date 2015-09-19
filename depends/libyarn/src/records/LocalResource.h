/*
 * LocalResource.h
 *
 *  Created on: Jul 30, 2014
 *      Author: bwang
 */

#ifndef LOCALRESOURCE_H_
#define LOCALRESOURCE_H_

#include <iostream>
#include "YARN_yarn_protos.pb.h"
#include "URL.h"
#include "LocalResourceType.h"
#include "LocalResourceVisibility.h"

using namespace std;
using namespace hadoop::yarn;
namespace libyarn {

class LocalResource {

//	message LocalResourceProto {
//	  optional URLProto resource = 1;
//	  optional int64 size = 2;
//	  optional int64 timestamp = 3;
//	  optional LocalResourceTypeProto type = 4;
//	  optional LocalResourceVisibilityProto visibility = 5;
//	  optional string pattern = 6;
//	}

public:
	LocalResource();
	LocalResource(const LocalResourceProto &proto);
	virtual ~LocalResource();

	LocalResourceProto& getProto();

	URL getResource();
	void setResource(URL &resource);

	long getSize();
	void setSize(long size);

	long getTimestamp();
	void setTimestamp(long timestamp);

	LocalResourceType getType();
	void setType(LocalResourceType &type);

	LocalResourceVisibility getVisibility();
	void setVisibility(LocalResourceVisibility &visibility);

	string getPattern();
	void setPattern(string &pattern);

private:
	LocalResourceProto localReProto;
};

} /* namespace libyarn */

#endif /* LOCALRESOURCE_H_ */
