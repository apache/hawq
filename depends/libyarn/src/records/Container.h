/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */
#ifndef CONTAINER_H_
#define CONTAINER_H_

#include "YARN_yarn_protos.pb.h"

#include "ContainerId.h"
#include "NodeId.h"
#include "Resource.h"
#include "Priority.h"
#include "Token.h"


using namespace std;
using namespace hadoop::yarn;
using namespace hadoop::common;

namespace libyarn {

/*
message ContainerProto {
  optional ContainerIdProto id = 1;
  optional NodeIdProto nodeId = 2;
  optional string node_http_address = 3;
  optional ResourceProto resource = 4;
  optional PriorityProto priority = 5;
  optional hadoop.common.TokenProto container_token = 6;
}
*/

class Container {
public:
	Container();
	Container(const ContainerProto &proto);
	virtual ~Container();

	ContainerProto& getProto();

	void setId(ContainerId &id);
	ContainerId getId();

	void setNodeId(NodeId &nodeId);
	NodeId getNodeId();

	void setNodeHttpAddress(string &httpAddress);
	string getNodeHttpAddress();

	void setResource(Resource &resource);
	Resource getResource();

	void setPriority(Priority &priority);
	Priority getPriority();


	void setContainerToken(Token containerToken);
	Token getContainerToken();

private:
	ContainerProto containerProto;
};

} /* namespace libyarn */

#endif /* CONTAINER_H_ */
