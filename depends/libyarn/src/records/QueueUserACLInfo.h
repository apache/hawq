/*
 * QueueUserACLInfo.h
 *
 *  Created on: Jul 29, 2014
 *      Author: bwang
 */

#ifndef QUEUEUSERACLINFO_H_
#define QUEUEUSERACLINFO_H_

#include <list>
#include <iostream>
#include "YARN_yarn_protos.pb.h"
#include "QueueACL.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {

//message QueueUserACLInfoProto {
//  optional string queueName = 1;
//  repeated QueueACLProto userAcls = 2;
//}

class QueueUserACLInfo {
public:
	QueueUserACLInfo();
	QueueUserACLInfo(const QueueUserACLInfoProto &proto);
	virtual ~QueueUserACLInfo();

	QueueUserACLInfoProto& getProto();

	string getQueueName();
	void setQueueName(string &queueName);

	list<QueueACL> getUserAcls();
	void setUserAcls(list<QueueACL> &userAclsList);
private:
	QueueUserACLInfoProto infoProto;
};

} /* namespace libyarn */

#endif /* QUEUEUSERACLINFO_H_ */
