#include <unistd.h>
#include <cstring>
#include <string>
#include <limits>
#include "xml_parser.h"

using std::string;

namespace hawq {
namespace test {

XmlConfig::XmlConfig(string p) :
    path(p) {
  parse();
}

bool XmlConfig::open() {
  if (access(path.c_str(), R_OK)) {
    return false;
  }

  doc = xmlReadFile(path.c_str(), nullptr, 0);
  if (doc == nullptr) {
    return false;
  }

  return true;
}

void XmlConfig::closeNotSave() {
    xmlFreeDoc(doc);
}

void XmlConfig::closeAndSave() {
    xmlSaveFormatFile(path.c_str(), doc, 0); 
    xmlFreeDoc(doc);
}

bool XmlConfig::parse() {
  LIBXML_TEST_VERSION kv
  .clear();

  if (!open()) {
    return false;
  }
  try {
    readConfigItems(doc);
    closeNotSave();
    return true;
  } catch (...) {
    closeNotSave();
    return false;
  }
}

void XmlConfig::readConfigItems(xmlDocPtr doc) {
  xmlNodePtr root, curNode;
  root = xmlDocGetRootElement(doc);
  if (root == nullptr || strcmp((const char *) root->name, "configuration")) {
    return ;
  }
  // for each property
  for (curNode = root->children; curNode != nullptr; curNode = curNode->next) {
    if (curNode->type != XML_ELEMENT_NODE) {
      continue;
    }
    if (strcmp((const char *) curNode->name, "property")) {
      return;
    }
    readConfigItem(curNode->children);
  }
}

void XmlConfig::readConfigItem(xmlNodePtr root) {
  string key, value, scope;
  xmlNodePtr curNode;
  bool hasName = false, hasValue = false, hasScope = false;
  for (curNode = root; curNode != nullptr; curNode = curNode->next) {
    if (curNode->type != XML_ELEMENT_NODE) {
      continue;
    }
    if (!hasName && !strcmp((const char *) curNode->name, "name")) {
      if (curNode->children != nullptr
          && XML_TEXT_NODE == curNode->children->type) {
        key = (const char *) curNode->children->content;
        hasName = true;
      }
    } else if (!hasValue && !strcmp((const char *) curNode->name, "value")) {
      if (curNode->children != nullptr
          && XML_TEXT_NODE == curNode->children->type) {
        value = (const char *) curNode->children->content;
        hasValue = true;
      }
    } else {
      continue;
    }
  }
  if (hasName && hasValue) {
    kv[key] = value;
    return;
  } else if (hasName && hasValue) {
    return;
  } else if (hasName && hasScope) {
    return;
  } else if (hasName) {
    return;
  }
}

bool XmlConfig::setString(const string &key,
                          const string &value,
                          bool save) {
  bool result = false;

  if (save) {
    if (!open()) {
      return false;
    }
  }
  try {
    result = writeConfigItem(doc, key, value);
    if (save) {
      closeAndSave();
    }
  } catch (...) {
    if (save) {
      closeNotSave();
    }
  }
  return result;
}

bool XmlConfig::setString(const string &key,
                          const string &value) {
  return setString(key, value, true);
}

bool XmlConfig::writeConfigItem(xmlDocPtr doc,
                                const string &key,
                                const string &value) {
  xmlNodePtr root, curNode, curNodeChild, findNode;
  root = xmlDocGetRootElement(doc);
  bool findkey = false;
  if (root == nullptr || strcmp((const char *) root->name, "configuration")) {
    return false;
  }
  // for each property
  for (curNode = root->children; curNode != nullptr; curNode = curNode->next) {
    if (curNode->type != XML_ELEMENT_NODE) {
      continue;
    }
    if (strcmp((const char *) curNode->name, "property")) {
      return false;
    }
    curNodeChild = curNode->children;
    for (curNodeChild = curNode->children; curNodeChild != nullptr; curNodeChild = curNodeChild->next) {
      if (curNodeChild->type != XML_ELEMENT_NODE) {
        continue;
      }
      if (!strcmp((const char *) curNodeChild->name, "name")) {
        if (curNodeChild->children != nullptr
            && XML_TEXT_NODE == curNodeChild->children->type
            && !strcmp((const char *) curNodeChild->children->content, key.c_str())) {
          findNode = curNode;
          findkey = true;
        } else {
          if (findkey) {
            xmlNewTextChild(findNode, NULL, BAD_CAST "value", BAD_CAST value.c_str());
            return true;
          }
        }
      } else if (!strcmp((const char *) curNodeChild->name, "value")) {
        if (findkey
            && curNodeChild->children != nullptr
            && XML_TEXT_NODE == curNodeChild->children->type) {
          xmlNodeSetContent(curNodeChild->children, (xmlChar*) const_cast<char*>(value.c_str()));
          return true;
        }
      } else {
        continue;
      }
    }
  }

  xmlNodePtr newNode = xmlNewNode(NULL, BAD_CAST "property");
  xmlAddChild(root, newNode);
  xmlNewTextChild(newNode, NULL, BAD_CAST "name", BAD_CAST key.c_str());
  xmlNewTextChild(newNode, NULL, BAD_CAST "value", BAD_CAST value.c_str());
  return true;
}

const string XmlConfig::getString(const char *key) {
  XmlConfigMapIterator it = kv.find(key);
  if (kv.end() == it) {
    return "";
  }
  return it->second;
}

const string XmlConfig::getString(const char *key, const char *def) {
  XmlConfigMapIterator it = kv.find(key);
  if (kv.end() == it) {
    return string(def);
  } else {
    return it->second;
  }
}

const string XmlConfig::getString(const string & key) {
  return getString(key.c_str());
}

const string XmlConfig::getString(const string & key,
                                  const string & def) {
  return getString(key.c_str(), def.c_str());
}

int64_t XmlConfig::getInt64(const char *key) {
  int64_t retval;
  XmlConfigMapIterator it = kv.find(key);
  if (kv.end() == it) {
    return 0;
  }
  retval = strToInt64(it->second.c_str());
  return retval;
}

int64_t XmlConfig::getInt64(const char *key, int64_t def) {
  int64_t retval;
  XmlConfigMapIterator it = kv.find(key);
  if (kv.end() == it) {
    return def;
  }
  retval = strToInt64(it->second.c_str());
  return retval;
}

int32_t XmlConfig::getInt32(const char *key) {
  int32_t retval;
  XmlConfigMapIterator it = kv.find(key);
  if (kv.end() == it) {
    return 0;
  }
  retval = strToInt32(it->second.c_str());
  return retval;
}

int32_t XmlConfig::getInt32(const char *key, int32_t def) {
  int32_t retval;
  XmlConfigMapIterator it = kv.find(key);
  if (kv.end() == it) {
    return def;
  }
  retval = strToInt32(it->second.c_str());
  return retval;
}

double XmlConfig::getDouble(const char *key) {
  double retval;
  XmlConfigMapIterator it = kv.find(key);
  if (kv.end() == it) {
    return 0.0;
  }
  retval = strToDouble(it->second.c_str());
  return retval;
}

double XmlConfig::getDouble(const char *key, double def) {
  double retval;
  XmlConfigMapIterator it = kv.find(key);
  if (kv.end() == it) {
    return def;
  }
  retval = strToDouble(it->second.c_str());
  return retval;
}

bool XmlConfig::getBool(const char *key) {
  bool retval;
  XmlConfigMapIterator it = kv.find(key);
  if (kv.end() == it) {
      return false;
  }
  retval = strToBool(it->second.c_str());
  return retval;
}

bool XmlConfig::getBool(const char *key, bool def) {
  bool retval;
  XmlConfigMapIterator it = kv.find(key);
  if (kv.end() == it) {
    return def;
  }
  retval = strToBool(it->second.c_str());
  return retval;
}

int64_t XmlConfig::strToInt64(const char *str) {
  int64_t retval;
  char *end = nullptr;
  retval = strtoll(str, &end, 0);
  return retval;
}

int32_t XmlConfig::strToInt32(const char *str) {
  int32_t retval;
  char *end = nullptr;
  retval = strtoll(str, &end, 0);
  return retval;
}

bool XmlConfig::strToBool(const char *str) {
  bool retval = false;
  if (!strcasecmp(str, "true") || !strcmp(str, "1")) {
    retval = true;
  } else if (!strcasecmp(str, "false") || !strcmp(str, "0")) {
    retval = false;
  } else {
    return false;
  }
  return retval;
}

double XmlConfig::strToDouble(const char *str) {
  double retval;
  char *end = nullptr;
  retval = strtod(str, &end);
  return retval;
}

} // namespace test
} // namespace hawq
