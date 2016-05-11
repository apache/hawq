#include "xml-parser.h"

#include <limits>
#include <unistd.h>

XmlConfig::XmlConfig(const char *p) :
    path(p) {
  parse();
}

void XmlConfig::parse() {
  // the result document tree
  xmlDocPtr doc;
  LIBXML_TEST_VERSION kv
  .clear();

  if (access(path.c_str(), R_OK)) {
    return;
  }

  // parse the file
  doc = xmlReadFile(path.c_str(), nullptr, 0);
  if (doc == nullptr) {
    return;
  }
  try {
    // printf("ffff3\n");
    readConfigItems(doc);
    // printf("ffff4\n");
    xmlFreeDoc(doc);
  } catch (...) {
    xmlFreeDoc(doc);
    // LOG_ERROR(ERRCODE_INTERNAL_ERROR, "libxml internal error");
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
  std::string key, value, scope;
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

const char *XmlConfig::getString(const char *key) {
  XmlConfigMapIterator it = kv.find(key);

  if (kv.end() == it) {
    return "";
  }

  return it->second.c_str();
}

const char *XmlConfig::getString(const char *key, const char *def) {
  XmlConfigMapIterator it = kv.find(key);

  if (kv.end() == it) {
    return def;
  } else {
    return it->second.c_str();
  }
}

const char *XmlConfig::getString(const std::string &key) {
  return getString(key.c_str());
}

const char *XmlConfig::getString(const std::string &key,
    const std::string &def) {
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
