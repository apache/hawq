/*
 * ContainerExceptionMap.h
 *
 *  Created on: Jul 31, 2014
 *      Author: bwang
 */

#ifndef CONTAINEREXCEPTIONMAP_H_
#define CONTAINEREXCEPTIONMAP_H_

#include "SerializedException.h"
#include "ContainerId.h"
#include "YARN_yarn_protos.pb.h"
#include "YARN_yarn_service_protos.pb.h"

namespace libyarn {

//message ContainerExceptionMapProto {
//  optional ContainerIdProto container_id = 1;
//  optional SerializedExceptionProto exception = 2;
//}

class ContainerExceptionMap {
public:
	ContainerExceptionMap();
	ContainerExceptionMap(const ContainerExceptionMapProto &proto);
	virtual ~ContainerExceptionMap();

	ContainerExceptionMapProto& getProto();

	void setContainerId(ContainerId &cId);
	ContainerId getContainerId();

	void setSerializedException(SerializedException & exception);
	SerializedException getSerializedException();

private:
	ContainerExceptionMapProto ceProto;
};

} /* namespace libyarn */

#endif /* CONTAINEREXCEPTIONMAP_H_ */
