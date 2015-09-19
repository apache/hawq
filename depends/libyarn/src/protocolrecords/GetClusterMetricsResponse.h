/*
 * GetClusterMetricsResponse.h
 *
 *  Created on: Jul 28, 2014
 *      Author: bwang
 */

#ifndef GETCLUSTERMETRICSRESPONSE_H_
#define GETCLUSTERMETRICSRESPONSE_H_

#include "YARN_yarn_service_protos.pb.h"
#include "records/YarnClusterMetrics.h"

using namespace hadoop::yarn;

namespace libyarn {

//message GetClusterMetricsResponseProto {
//  optional YarnClusterMetricsProto cluster_metrics = 1;
//}

class GetClusterMetricsResponse {
public:
	GetClusterMetricsResponse();
	GetClusterMetricsResponse(const GetClusterMetricsResponseProto &proto);
	virtual ~GetClusterMetricsResponse();

	GetClusterMetricsResponseProto& getProto();

	YarnClusterMetrics getClusterMetrics();
	void setClusterMetrics(YarnClusterMetrics &clusterMetrics);

private:
	GetClusterMetricsResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* GETCLUSTERMETRICSRESPONSE_H_ */
