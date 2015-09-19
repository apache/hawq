/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */
#ifndef RESOURCEBLACKLISTREQUEST_H_
#define RESOURCEBLACKLISTREQUEST_H_

#include <list>
#include "YARN_yarn_protos.pb.h"

using namespace std;
using namespace hadoop::yarn;

namespace libyarn {

/*
message ResourceBlacklistRequestProto {
  repeated string blacklist_additions = 1;
  repeated string blacklist_removals = 2;
}
*/

class ResourceBlacklistRequest {
public:
	ResourceBlacklistRequest();
	ResourceBlacklistRequest(const ResourceBlacklistRequestProto &proto);
	virtual ~ResourceBlacklistRequest();

	ResourceBlacklistRequestProto& getProto();

	void setBlacklistAdditions(list<string> &additions);
	void addBlacklistAddition(string &addition);
	list<string> getBlacklistAdditions();

	void setBlacklistRemovals(list<string> &removals);
	void addBlacklistRemoval(string &removal);
	list<string> getBlacklistRemovals();

private:
	ResourceBlacklistRequestProto requestProto;
};

} /* namespace libyarn */

#endif /* RESOURCEBLACKLISTREQUEST_H_ */
