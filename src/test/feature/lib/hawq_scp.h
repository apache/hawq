#ifndef SRC_TEST_FEATURE_LIB_HAWQ_SCP_H_
#define SRC_TEST_FEATURE_LIB_HAWQ_SCP_H_

#include <string>

namespace hawq {
namespace test {

class HAWQScp
{
public:
	HAWQScp() = default;
	~HAWQScp() = default;
	HAWQScp(const HAWQScp&) = delete;
	HAWQScp& operator=(const HAWQScp&) = delete;

	bool copy(const std::string& host_list, const std::string& src_file, const std::string& dst_dir);
};

} // namespace test
} // namespace hawq

#endif   // SRC_TEST_FEATURE_LIB_HAWQ_SCP_H_
