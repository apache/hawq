/*
 * ApplicationACLMap.h
 *
 *  Created on: Jul 30, 2014
 *      Author: bwang
 */

#ifndef APPLICATIONACLMAP_H_
#define APPLICATIONACLMAP_H_

#include <iostream>
#include "YARN_yarn_protos.pb.h"
#include "ApplicationAccessType.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {

//message ApplicationACLMapProto {
//  optional ApplicationAccessTypeProto accessType = 1;
//  optional string acl = 2 [default = " "];
//}

class ApplicationACLMap {
public:
	ApplicationACLMap();
	ApplicationACLMap(const ApplicationACLMapProto &proto);
	virtual ~ApplicationACLMap();

	ApplicationACLMapProto& getProto();

	void setAccessType(ApplicationAccessType &accessType);
	ApplicationAccessType getAccessType();

	void setAcl(string &acl);
	string getAcl();

private:
	ApplicationACLMapProto appAclProto;
};

} /* namespace libyarn */

#endif /* APPLICATIONACLMAP_H_ */
