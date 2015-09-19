/*
 * AMCommand.h
 *
 *  Created on: Jul 16, 2014
 *      Author: bwang
 */
#ifndef TOKEN_H_
#define TOKEN_H_

#include "YARNSecurity.pb.h"

using namespace std;
using namespace hadoop::common;

namespace libyarn {
/*
message TokenProto {
  required bytes identifier = 1;
  required bytes password = 2;
  required string kind = 3;
  required string service = 4;
}
 */
class Token {
public:
	Token();
	Token(const TokenProto &proto);
	virtual ~Token();

	TokenProto& getProto();

	void setIdentifier(string &identifier);
	string getIdentifier();

	void setPassword(string &passwd);
	string getPassword();

	void setKind(string &kind);
	string getKind();

	void setService(string &service);
	string getService();

private:
	TokenProto tokenProto;
};

} /* namespace libyarn */

#endif /* TOKEN_H_ */
