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

#include "Priority.h"

namespace libyarn {

Priority::Priority() {
	priProto = PriorityProto::default_instance();
}

Priority::Priority(int32_t priority) {
	Priority();
	priProto.set_priority(priority);
}

Priority::Priority(const PriorityProto &proto) : priProto (proto) {
}

Priority::~Priority() {
}

PriorityProto& Priority::getProto(){
	return priProto;
}

void Priority::setPriority(int32_t priority){
	priProto.set_priority(priority);
}

int32_t Priority::getPriority(){
	return priProto.priority();
}

} /* namespace libyarn */
