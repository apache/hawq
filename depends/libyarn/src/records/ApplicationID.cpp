#include "ApplicationID.h"

namespace libyarn {

ApplicationID::ApplicationID() {
	appIdProto = ApplicationIdProto::default_instance();
}

ApplicationID::ApplicationID(const ApplicationIdProto &proto) : appIdProto(proto) {
}

ApplicationID::ApplicationID(const ApplicationID &applicationId){
	appIdProto = applicationId.appIdProto;
}

ApplicationID::~ApplicationID() {
}

ApplicationIdProto& ApplicationID::getProto() {
	return appIdProto;
}

void ApplicationID::setId(int32_t id) {
	appIdProto.set_id(id);
}

int ApplicationID::getId() {
	return appIdProto.id();
}

void ApplicationID::setClusterTimestamp(int64_t timestamp) {
	appIdProto.set_cluster_timestamp(timestamp);
}

int64_t ApplicationID::getClusterTimestamp() {
	return appIdProto.cluster_timestamp();
}

} /* namespace libyarn */

