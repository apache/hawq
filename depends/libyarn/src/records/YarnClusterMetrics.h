/*
 * YarnClusterMetrics.h
 *
 *  Created on: Jul 28, 2014
 *      Author: bwang
 */

#ifndef YARNCLUSTERMETRICS_H_
#define YARNCLUSTERMETRICS_H_

#include "YARN_yarn_protos.pb.h"

using namespace hadoop::yarn;

namespace libyarn {

//message YarnClusterMetricsProto {
//  optional int32 num_node_managers = 1;
//}

class YarnClusterMetrics {
public:
	YarnClusterMetrics();
	YarnClusterMetrics(const YarnClusterMetricsProto &proto);
	virtual ~YarnClusterMetrics();

	YarnClusterMetricsProto& getProto();

	void setNumNodeManagers(int32_t numNodeManagers);
	int32_t getNumNodeManagers();

private:
	YarnClusterMetricsProto metricsProto;
};

} /* namespace libyarn */

#endif /* YARNCLUSTERMETRICS_H_ */
