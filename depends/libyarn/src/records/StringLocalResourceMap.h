/*
 * StringLocalResourceMap.h
 *
 *  Created on: Jul 30, 2014
 *      Author: bwang
 */

#ifndef STRINGLOCALRESOURCEMAP_H_
#define STRINGLOCALRESOURCEMAP_H_

#include "YARN_yarn_protos.pb.h"
#include "LocalResource.h"

using namespace std;
using namespace hadoop::yarn;
namespace libyarn {

class StringLocalResourceMap {

//	message StringLocalResourceMapProto {
//		optional string key = 1;
//		optional LocalResourceProto value = 2;
//	}

public:
	StringLocalResourceMap();
	StringLocalResourceMap(const StringLocalResourceMapProto &proto);
	virtual ~StringLocalResourceMap();

	StringLocalResourceMapProto& getProto();

	void setKey(string &key);
	string getKey();

	void setLocalResource(LocalResource &resource);
	LocalResource getLocalResource();

private:
	StringLocalResourceMapProto localResourceProto;
};

} /* namespace libyarn */

#endif /* STRINGLOCALRESOURCEMAP_H_ */
