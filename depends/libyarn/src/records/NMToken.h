/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */
#ifndef NMTOKEN_H_
#define NMTOKEN_H_

#include "YARN_yarn_protos.pb.h"
#include "YARN_yarn_service_protos.pb.h"

#include "NodeId.h"
#include "Token.h"

using namespace hadoop::yarn;
using namespace hadoop::common;

namespace libyarn {

/*
message NMTokenProto {
  optional NodeIdProto nodeId = 1;
  optional hadoop.common.TokenProto token = 2;
}
*/

class NMToken {
public:
	NMToken();
	NMToken(const NMTokenProto &proto);
	virtual ~NMToken();

	NMTokenProto& getProto();

	void setNodeId(NodeId &nodeId);
	NodeId getNodeId();

	void setToken(Token token);
	Token getToken();

private:
	NMTokenProto nmTokenProto;
};

} /* namespace libyarn */

#endif /* NMTOKEN_H_ */
