#ifndef SRC_TEST_FEATURE_LIB_FILE_REPLACE_H_
#define SRC_TEST_FEATURE_LIB_FILE_REPLACE_H_

#include <string>
#include <unordered_map>

namespace hawq {
namespace test {

class FileReplace
{
public:
	FileReplace() = default;
	~FileReplace() = default;
	FileReplace(const FileReplace&) = delete;
	FileReplace& operator=(const FileReplace&) = delete;

	void replace(const std::string& file_src,
	             const std::string& file_dst,
	             const std::unordered_map<std::string, std::string>& strs_src_dst);

private:
	std::string replaceAllOccurrences(std::string str, const std::string& src, const std::string& dst);
};

} // namespace test
} // namespace hawq

#endif   // SRC_TEST_FEATURE_LIB_FILE_REPLACE_H_
