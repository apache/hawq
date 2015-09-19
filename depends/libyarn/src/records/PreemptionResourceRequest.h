/*
 * PreemptionResourceRequest.h
 *
 *  Created on: Jul 20, 2014
 *      Author: bwang
 */

#ifndef PREEMPTIONRESOURCEREQUEST_H_
#define PREEMPTIONRESOURCEREQUEST_H_

#include "YARN_yarn_protos.pb.h"

#include "ResourceRequest.h"

using namespace hadoop;
using namespace yarn;
namespace libyarn {

class PreemptionResourceRequest {
public:
	PreemptionResourceRequest();
	PreemptionResourceRequest(const PreemptionResourceRequestProto &proto);
	virtual ~PreemptionResourceRequest();

	PreemptionResourceRequestProto& getProto();
	ResourceRequest getResourceRequest();
	void setResourceRequest(ResourceRequest &rr);
private:
	PreemptionResourceRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* PREEMPTIONRESOURCEREQUEST_H_ */

//message PreemptionResourceRequestProto {
//  optional ResourceRequestProto resource = 1;
//}
