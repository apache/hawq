/*
 * GetClusterMetricsRequest.h
 *
 *  Created on: Jul 28, 2014
 *      Author: bwang
 */

#ifndef GETCLUSTERMETRICSREQUEST_H_
#define GETCLUSTERMETRICSREQUEST_H_

#include "YARN_yarn_service_protos.pb.h"

using namespace hadoop::yarn;

namespace libyarn {

//message GetClusterMetricsRequestProto {
//}

class GetClusterMetricsRequest {
public:
	GetClusterMetricsRequest();
	GetClusterMetricsRequest(const GetClusterMetricsRequestProto &proto);
	virtual ~GetClusterMetricsRequest();

	GetClusterMetricsRequestProto& getProto();

private:
	GetClusterMetricsRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* GETCLUSTERMETRICSREQUEST_H_ */
