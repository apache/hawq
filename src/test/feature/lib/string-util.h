#ifndef SRC_TEST_FEATURE_LIB_STRING_UTIL_H_
#define SRC_TEST_FEATURE_LIB_STRING_UTIL_H_


#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

class StringUtil {
 public:
  StringUtil() {}
  ~StringUtil() {}

  static bool iequals(const std::string &str1, const std::string &str2);
  static void replace(std::string *subject, const std::string &search,
                      const std::string &replace);
  static std::string regexReplace(std::string *subject,
                                  const std::string &pattern,
                                  const std::string &replace);
  static void toLower(std::string *str);
  static std::string lower(const std::string &str);
  static std::string &trim(std::string &s);         // NOLINT
  static std::string &trimNewLine(std::string &s);  // NOLINT
  static std::vector<std::string> split(const std::string &s, char delimiter);
  static bool StartWith(const std::string &str, const std::string &strStart);

  template <typename T>
  static std::string toStringWithPrecision(const T value, const int n) {
    std::ostringstream out;
    out << std::setiosflags(std::ios::fixed) << std::setprecision(n) << value;
    return out.str();
  }
};
#endif /* SRC_TEST_FEATURE_LIB_STRING_UTIL_H_ */
