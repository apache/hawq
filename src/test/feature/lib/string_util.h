#ifndef HAWQ_SRC_TEST_FEATURE_LIB_STRING_UTIL_H_
#define HAWQ_SRC_TEST_FEATURE_LIB_STRING_UTIL_H_

#include <string>
#include <vector>

namespace hawq {
namespace test {

bool iequals(const std::string &, const std::string &);

void replace(std::string &, const std::string &, const std::string &);

void toLower(std::string &);

std::string lower(const std::string &);

std::string &trim(std::string &);

std::string &trimNewLine(std::string &);

std::vector<std::string> split(const std::string &, char);

std::string regexReplace(std::string &, const std::string &, const std::string &);

bool startsWith(const std::string &, const std::string &);

bool endsWith(const std::string &, const std::string &);

} // namespace test
} // namespace hawq 

#endif
