/*
 * StartContainerResponse.h
 *
 *  Created on: Jul 22, 2014
 *      Author: jcao
 */

#ifndef STARTCONTAINERRESPONSE_H_
#define STARTCONTAINERRESPONSE_H_

#include <list>
#include "YARN_yarn_service_protos.pb.h"
#include "records/StringBytesMap.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {

/*
message StartContainerResponseProto {
  repeated StringBytesMapProto services_meta_data = 1;
}
 */
class StartContainerResponse {
public:
	StartContainerResponse();
	StartContainerResponse(const StartContainerResponseProto &proto);
	virtual ~StartContainerResponse();

	StartContainerResponseProto& getProto();

	void setServicesMetaData(list<StringBytesMap> datas);
	list<StringBytesMap> getSerivcesMetaData();

private:
	StartContainerResponseProto responseProto;
};

} /* namespace libyarn */

#endif /* STARTCONTAINERRESPONSE_H_ */
