/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */
#ifndef NODEID_H_
#define NODEID_H_

#include "YARN_yarn_protos.pb.h"

using namespace std;

using namespace hadoop::yarn;

namespace libyarn {
/*
message NodeIdProto {
  optional string host = 1;
  optional int32 port = 2;
}
*/
class NodeId {
public:
	NodeId();
	NodeId(const NodeIdProto &proto);
	virtual ~NodeId();

	NodeIdProto& getProto();

	void setHost(string &host);
	string getHost();

	void setPort(int32_t port);
	int32_t getPort();

private:
	NodeIdProto nodeIdProto;
};

} /* namespace libyarn */

#endif /* NODEID_H_ */
