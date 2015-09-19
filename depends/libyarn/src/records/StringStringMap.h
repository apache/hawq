/*
 * StringStringMap.h
 *
 *  Created on: Jul 30, 2014
 *      Author: bwang
 */

#ifndef STRINGSTRINGMAP_H_
#define STRINGSTRINGMAP_H_

#include <iostream>
#include "YARN_yarn_protos.pb.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {

//message StringStringMapProto {
//  optional string key = 1;
//  optional string value = 2;
//}

class StringStringMap {
public:
	StringStringMap();
	StringStringMap(const StringStringMapProto &proto);
	virtual ~StringStringMap();

	StringStringMapProto& getProto();

	void setKey(string &key);
	string getKey();

	void setValue(string &value);
	string getValue();

private:
	StringStringMapProto ssMapProto;
};

} /* namespace libyarn */

#endif /* STRINGSTRINGMAP_H_ */
