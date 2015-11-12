/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
