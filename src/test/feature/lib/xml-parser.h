#ifndef SRC_TEST_FEATURE_LIB_XML_PARSER_H_
#define SRC_TEST_FEATURE_LIB_XML_PARSER_H_


#include <cstring>
#include <string>
#include <unordered_map>

#include "libxml/parser.h"

typedef std::unordered_map<std::string, std::string> XmlConfigMap;
typedef std::unordered_map<std::string, std::string>::const_iterator
    XmlConfigMapIterator;

class XmlConfig {
 public:
  explicit XmlConfig(const char *p);

  // parse the configuration file
  void parse();

  // @param key The key of the configuration item
  // @ def The default value
  // @ return The value of configuration item
  const char *getString(const char *key);

  const char *getString(const char *key, const char *def);

  const char *getString(const std::string &key);

  const char *getString(const std::string &key, const std::string &def);

  int64_t getInt64(const char *key);

  int64_t getInt64(const char *key, int64_t def);

  int32_t getInt32(const char *key);

  int32_t getInt32(const char *key, int32_t def);

  double getDouble(const char *key);

  double getDouble(const char *key, double def);

  bool getBool(const char *key);

  bool getBool(const char *key, bool def);

  XmlConfigMap *getConfigMap() { return &kv; }

 private:
  std::string path;
  XmlConfigMap kv;  // key2Value

  void readConfigItems(xmlDocPtr doc);
  void readConfigItem(xmlNodePtr root);
  int64_t strToInt64(const char *str);
  int32_t strToInt32(const char *str);
  bool strToBool(const char *str);
  double strToDouble(const char *str);
};


#endif /* SRC_TEST_FEATURE_LIB_XML_PARSER_H_ */
