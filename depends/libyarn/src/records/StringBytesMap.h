/*
 * StringBytesMap.h
 *
 *  Created on: Jul 22, 2014
 *      Author: jcao
 */

#ifndef STRINGBYTESMAP_H_
#define STRINGBYTESMAP_H_

#include <iostream>
#include "YARN_yarn_protos.pb.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {
/*
message StringBytesMapProto {
  optional string key = 1;
  optional bytes value = 2;
}
 */
class StringBytesMap {
public:
	StringBytesMap();
	StringBytesMap(const StringBytesMapProto &proto);
	virtual ~StringBytesMap();

	StringBytesMapProto& getProto();

	void setKey(string &key);
	string getKey();

	void setValue(string &value);
	string getValue();

private:
	StringBytesMapProto mapProto;
};

} /* namespace libyarn */

#endif /* STRINGBYTESMAP_H_ */
