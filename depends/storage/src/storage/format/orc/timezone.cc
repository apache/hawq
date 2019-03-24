/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <cerrno>
#include <iostream>
#include <map>
#include <sstream>

#include "dbcommon/log/logger.h"
#include "storage/format/orc/timezone.h"

namespace orc {

// Find the position that is the closest and less than or equal to the
// target.
// @return -1 if the target < array[0] or array is empty or
//          i if array[i] <= target and (i == n or array[i] < array[i+1])
int64_t binarySearch(const std::vector<int64_t>& array, int64_t target) {
  uint64_t size = array.size();
  if (size == 0) {
    return -1;
  }
  uint64_t min = 0;
  uint64_t max = size - 1;
  uint64_t mid = (min + max) / 2;
  while ((array[mid] != target) && (min < max)) {
    if (array[mid] < target) {
      min = mid + 1;
    } else if (mid == 0) {
      max = 0;
    } else {
      max = mid - 1;
    }
    mid = (min + max) / 2;
  }
  if (target < array[mid]) {
    return static_cast<int64_t>(mid) - 1;
  } else {
    return static_cast<int64_t>(mid);
  }
}

FutureRule::~FutureRule() {
  // PASS
}

std::unique_ptr<FutureRule> parseFutureRule(const std::string& ruleString) {
  std::unique_ptr<FutureRule> result(new FutureRuleImpl());
  FutureRuleParser parser(ruleString,
                          dynamic_cast<FutureRuleImpl*>(result.get()));
  return result;
}

FutureRuleImpl::~FutureRuleImpl() {
  // PASS
}

bool FutureRuleImpl::isDefined() const { return ruleString.size() > 0; }

const TimezoneVariant& FutureRuleImpl::getVariant(int64_t clk) const {
  if (!hasDst) {
    return standard;
  } else {
    int64_t adjusted = clk % SECONDS_PER_400_YEARS;
    if (adjusted < 0) {
      adjusted += SECONDS_PER_400_YEARS;
    }
    int64_t idx = binarySearch(offsets, adjusted);
    if (startInStd == (idx % 2 == 0)) {
      return standard;
    } else {
      return dst;
    }
  }
}

void FutureRuleImpl::print(std::ostream* out) const {
  if (isDefined()) {
    *out << "  Future rule: " << ruleString << "\n";
    *out << "  standard " << standard.toString() << "\n";
    if (hasDst) {
      *out << "  dst " << dst.toString() << "\n";
      *out << "  start " << start.toString() << "\n";
      *out << "  end " << end.toString() << "\n";
    }
  }
}

VersionParser::~VersionParser() {
  // PASS
}

static uint32_t decode32(const unsigned char* ptr) {
  return static_cast<uint32_t>(ptr[0] << 24) |
         static_cast<uint32_t>(ptr[1] << 16) |
         static_cast<uint32_t>(ptr[2] << 8) | static_cast<uint32_t>(ptr[3]);
}

class Version1Parser : public VersionParser {
 public:
  virtual ~Version1Parser();

  uint64_t getVersion() const override { return 1; }

  // Get the number of bytes
  uint64_t getTimeSize() const override { return 4; }

  // Parse the time at the given location.
  int64_t parseTime(const unsigned char* ptr) const override {
    // sign extend from 32 bits
    return static_cast<int32_t>(decode32(ptr));
  }

  std::string parseFutureString(const unsigned char*, uint64_t,
                                uint64_t) const override {
    return "";
  }
};

Version1Parser::~Version1Parser() {
  // PASS
}

class Version2Parser : public VersionParser {
 public:
  virtual ~Version2Parser();

  uint64_t getVersion() const override { return 2; }

  // Get the number of bytes
  uint64_t getTimeSize() const override { return 8; }

  // Parse the time at the given location.
  int64_t parseTime(const unsigned char* ptr) const override {
    return static_cast<int64_t>(decode32(ptr)) << 32 | decode32(ptr + 4);
  }

  std::string parseFutureString(const unsigned char* ptr, uint64_t offset,
                                uint64_t length) const override {
    return std::string(reinterpret_cast<const char*>(ptr) + offset + 1,
                       length - 2);
  }
};

Version2Parser::~Version2Parser() {
  // PASS
}

static std::map<std::string, Timezone*> timezoneCache;

Timezone::~Timezone() {
  // PASS
}

TimezoneImpl::TimezoneImpl(const std::string& _filename,
                           const std::vector<unsigned char> buffer)
    : filename(_filename) {
  parseZoneFile(&buffer[0], 0, buffer.size(), Version1Parser());
  // Build the literal for the ORC epoch
  // 2015 Jan 1 00:00:00
  tm epochStruct;
  epochStruct.tm_sec = 0;
  epochStruct.tm_min = 0;
  epochStruct.tm_hour = 0;
  epochStruct.tm_mday = 1;
  epochStruct.tm_mon = 0;
  epochStruct.tm_year = 2015 - 1900;
  epochStruct.tm_isdst = 0;
  time_t utcEpoch = timegm(&epochStruct);
  epoch = utcEpoch - getVariant(utcEpoch).gmtOffset;
}

const char* getTimezoneDirectory() {
  const char* dir = getenv("TZDIR");
  if (!dir) {
    dir = DEFAULT_TZDIR;
  }
  return dir;
}

// Get a timezone by absolute filename.
// Results are cached.
const Timezone& getTimezoneByFilename(const std::string& filename) {
  std::map<std::string, Timezone*>::iterator itr = timezoneCache.find(filename);
  if (itr != timezoneCache.end()) {
    return *(itr->second);
  }
  int in = open(filename.c_str(), O_RDONLY);
  if (in == -1) {
    std::stringstream buffer;
    buffer << "failed to open " << filename << " - " << strerror(errno);
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "%s", buffer.str().c_str());
  }
  struct stat fileInfo;
  if (fstat(in, &fileInfo) == -1) {
    std::stringstream buffer;
    buffer << "failed to stat " << filename << " - " << strerror(errno);
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "%s", buffer.str().c_str());
  }
  if ((fileInfo.st_mode & S_IFMT) != S_IFREG) {
    std::stringstream buffer;
    buffer << "non-file in tzfile reader " << filename;
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "%s", buffer.str().c_str());
  }
  size_t size = static_cast<size_t>(fileInfo.st_size);
  std::vector<unsigned char> buffer(size);
  size_t posn = 0;
  while (posn < size) {
    ssize_t ret = read(in, &buffer[posn], size - posn);
    if (ret == -1) {
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "Failure to read timezone file %s - %s",
                filename.c_str(), strerror(errno));
    }
    posn += static_cast<size_t>(ret);
  }
  if (close(in) == -1) {
    std::stringstream err;
    err << "failed to close " << filename << " - " << strerror(errno);
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "%s", err.str().c_str());
  }
  Timezone* result = new TimezoneImpl(filename, buffer);
  timezoneCache[filename] = result;
  return *result;
}

// Get the local timezone.
const Timezone& getLocalTimezone() {
  return getTimezoneByFilename(LOCAL_TIMEZONE);
}

// Get a timezone by name (eg. America/Los_Angeles).
// Results are cached.
const Timezone& getTimezoneByName(const std::string& zone) {
  std::string filename(getTimezoneDirectory());
  filename += "/";
  filename += zone;
  return getTimezoneByFilename(filename);
}

// Parse a set of bytes as a timezone file as if they came from filename.
std::unique_ptr<Timezone> getTimezone(const std::string& filename,
                                      const std::vector<unsigned char>& b) {
  return std::unique_ptr<Timezone>(new TimezoneImpl(filename, b));
}

TimezoneImpl::~TimezoneImpl() {
  // PASS
}

void TimezoneImpl::parseTimeVariants(const unsigned char* ptr,
                                     uint64_t variantOffset,
                                     uint64_t variantCount, uint64_t nameOffset,
                                     uint64_t nameCount) {
  for (uint64_t variant = 0; variant < variantCount; ++variant) {
    variants[variant].gmtOffset =
        static_cast<int32_t>(decode32(ptr + variantOffset + 6 * variant));
    variants[variant].isDst = ptr[variantOffset + 6 * variant + 4];
    uint nameStart = ptr[variantOffset + 6 * variant + 5];
    if (nameStart >= nameCount) {
      std::stringstream buffer;
      buffer << "name out of range in variant " << variant << " - " << nameStart
             << " >= " << nameCount;
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "%s", buffer.str().c_str());
    }
    variants[variant].name = std::string(reinterpret_cast<const char*>(ptr) +
                                         nameOffset + nameStart);
  }
}

//
// Parse the zone file to get the bits we need.
// There are two versions of the timezone file:
//
// Version 1(version = 0x00):
//   Magic(version)
//   Header
//   TransitionTimes(4 byte)
//   TransitionRules
//   Rules
//   LeapSeconds(4 byte)
//   IsStd
//   IsGmt
//
// Version2:
//   Version1(0x32) = a version 1 copy of the data for old clients
//   Magic(0x32)
//   Header
//   TransitionTimes(8 byte)
//   TransitionRules
//   Rules
//   LeapSeconds(8 byte)
//   IsStd
//   IsGmt
//   FutureString
void TimezoneImpl::parseZoneFile(const unsigned char* ptr,
                                 uint64_t sectionOffset, uint64_t fileLength,
                                 const VersionParser& versionParser) {
  const uint64_t magicOffset = sectionOffset + 0;
  const uint64_t headerOffset = magicOffset + 20;

  // check for validity before we start parsing
  if (fileLength < headerOffset + 6 * 4 ||
      strncmp(reinterpret_cast<const char*>(ptr) + magicOffset, "TZif", 4) !=
          0) {
    std::stringstream buffer;
    buffer << "non-tzfile " << filename;
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "%s", buffer.str().c_str());
  }

  const uint64_t isGmtCount = decode32(ptr + headerOffset + 0);
  const uint64_t isStdCount = decode32(ptr + headerOffset + 4);
  const uint64_t leapCount = decode32(ptr + headerOffset + 8);
  const uint64_t timeCount = decode32(ptr + headerOffset + 12);
  const uint64_t variantCount = decode32(ptr + headerOffset + 16);
  const uint64_t nameCount = decode32(ptr + headerOffset + 20);

  const uint64_t timeOffset = headerOffset + 24;
  const uint64_t timeVariantOffset =
      timeOffset + versionParser.getTimeSize() * timeCount;
  const uint64_t variantOffset = timeVariantOffset + timeCount;
  const uint64_t nameOffset = variantOffset + variantCount * 6;
  const uint64_t sectionLength = nameOffset + nameCount +
                                 (versionParser.getTimeSize() + 4) * leapCount +
                                 isGmtCount + isStdCount;

  if (sectionLength > fileLength) {
    std::stringstream buffer;
    buffer << "tzfile too short " << filename << " needs " << sectionLength
           << " and has " << fileLength;
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "%s", buffer.str().c_str());
  }

  // if it is version 2, skip over the old layout and read the new one.
  if (sectionOffset == 0 && ptr[magicOffset + 4] != 0) {
    parseZoneFile(ptr, sectionLength, fileLength, Version2Parser());
    return;
  }
  version = versionParser.getVersion();
  variants.resize(variantCount);
  transitions.resize(timeCount);
  currentVariant.resize(timeCount);
  parseTimeVariants(ptr, variantOffset, variantCount, nameOffset, nameCount);
  bool foundAncient = false;
  for (uint64_t t = 0; t < timeCount; ++t) {
    transitions[t] = versionParser.parseTime(ptr + timeOffset +
                                             t * versionParser.getTimeSize());
    currentVariant[t] = ptr[timeVariantOffset + t];
    if (currentVariant[t] >= variantCount) {
      std::stringstream buffer;
      buffer << "tzfile rule out of range " << filename << " references rule "
             << currentVariant[t] << " of " << variantCount;
      LOG_ERROR(ERRCODE_INTERNAL_ERROR, "%s", buffer.str().c_str());
    }
    // find the oldest standard time and use that as the ancient value
    if (!foundAncient && !variants[currentVariant[t]].isDst) {
      foundAncient = true;
      ancientVariant = currentVariant[t];
    }
  }
  if (!foundAncient) {
    ancientVariant = 0;
  }
  futureRule = parseFutureRule(versionParser.parseFutureString(
      ptr, sectionLength, fileLength - sectionLength));

  // find the lower bound for applying the future rule
  if (futureRule->isDefined()) {
    if (timeCount > 0) {
      lastTransition = transitions[timeCount - 1];
    } else {
      lastTransition = INT64_MIN;
    }
  } else {
    lastTransition = INT64_MAX;
  }
}

const TimezoneVariant& TimezoneImpl::getVariant(int64_t clk) const {
  // if it is after the last explicit entry in the table,
  // use the future rule to get an answer
  if (clk > lastTransition) {
    return futureRule->getVariant(clk);
  } else {
    int64_t transition = binarySearch(transitions, clk);
    uint64_t idx;
    if (transition < 0) {
      idx = ancientVariant;
    } else {
      idx = currentVariant[static_cast<size_t>(transition)];
    }
    return variants[idx];
  }
}

void TimezoneImpl::print(std::ostream& out) const {
  out << "Timezone file: " << filename << "\n";
  out << "  Version: " << version << "\n";
  futureRule->print(&out);
  for (uint64_t r = 0; r < variants.size(); ++r) {
    out << "  Variant " << r << ": " << variants[r].toString() << "\n";
  }
  for (uint64_t t = 0; t < transitions.size(); ++t) {
    tm timeStruct;
    tm* result = nullptr;
    char buffer[25];
    if (sizeof(time_t) >= 8) {
      time_t val = transitions[t];
      result = gmtime_r(&val, &timeStruct);
      if (result) {
        strftime(buffer, sizeof(buffer), "%F %H:%M:%S", &timeStruct);
      }
    }
    std::cout << "  Transition: " << (result == nullptr ? "null" : buffer)
              << " (" << transitions[t] << ") -> "
              << variants[currentVariant[t]].name << "\n";
  }
}

TimezoneError::TimezoneError(const std::string& what)
    : std::runtime_error(what) {
  // PASS
}

TimezoneError::TimezoneError(const TimezoneError& other)
    : std::runtime_error(other) {
  // PASS
}

TimezoneError::~TimezoneError() noexcept {
  // PASS
}

}  // namespace orc
