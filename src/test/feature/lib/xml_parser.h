#ifndef HAWQ_SRC_TEST_FEATURE_LIB_XML_PARSER_H_
#define HAWQ_SRC_TEST_FEATURE_LIB_XML_PARSER_H_

#include <string>
#include <unordered_map>

#include "libxml/parser.h"

using XmlConfigMap = std::unordered_map<std::string, std::string>;
using XmlConfigMapIterator = std::unordered_map<std::string, std::string>::const_iterator;

namespace hawq {
namespace test {

class XmlConfig {

 public:
  explicit XmlConfig(std::string);
  
  // read an XML file into a tree
  bool open();

  // only free the XML document pointer
  void closeNotSave();

  // save the updated document to disk and free the XML document pointer
  void closeAndSave();

  // parse the configuration file
  bool parse();

  // @param key The key of the configuration item
  // @param value The updated value
  // @param save whether save the updated document to disk, if save is false, open() and closeAndSave() should be called additionally
  // @ return The value of configuration item
  bool setString(const std::string &key, const std::string &value, bool save);
  
  bool setString(const std::string &, const std::string &);

  // @param key The key of the configuration item
  // @ def The default value
  // @ return The value of configuration item
  const std::string getString(const char*);

  const std::string getString(const char *key, const char *def);

  const std::string getString(const std::string &);

  const std::string getString(const std::string & key, const std::string & def);

  int64_t getInt64(const char *);

  int64_t getInt64(const char *key, int64_t def);

  int32_t getInt32(const char *);

  int32_t getInt32(const char *key, int32_t def);

  double getDouble(const char *);

  double getDouble(const char *key, double def);

  bool getBool(const char *);

  bool getBool(const char *key, bool def);

  XmlConfigMap *getConfigMap() { return &kv; }

 private:
  void readConfigItems(xmlDocPtr doc);
  void readConfigItem(xmlNodePtr root);
  bool writeConfigItem(xmlDocPtr , const std::string &, const std::string &);
  int64_t strToInt64(const char *);
  int32_t strToInt32(const char *);
  bool strToBool(const char *);
  double strToDouble(const char *);
 
 private:
  std::string path;
  XmlConfigMap kv;  // key2Value
  xmlDocPtr doc;
}; // class XmlConfig

} // namespace test
} // namespace hawq

#endif /* SRC_TEST_FEATURE_LIB_XML_PARSER_H_ */
