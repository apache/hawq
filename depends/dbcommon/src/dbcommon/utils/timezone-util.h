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

#ifndef DBCOMMON_SRC_DBCOMMON_UTILS_TIMEZONE_UTIL_H_
#define DBCOMMON_SRC_DBCOMMON_UTILS_TIMEZONE_UTIL_H_

// This file is for timezone routines.

#include <stdint.h>

#include <ctime>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace dbcommon {

// default location of the timezone files
const char DEFAULT_TZDIR[] = "/usr/share/zoneinfo";

// location of a symlink to the local timezone
const char LOCAL_TIMEZONE[] = "/etc/localtime";

const int64_t MONTHS_PER_YEAR = 12;

// The number of days in each month in non-leap and leap years.
const int64_t DAYS_PER_MONTH[2][MONTHS_PER_YEAR] = {
    {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
    {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}};
const int64_t SECONDS_PER_HOUR = 60 * 60;
const int64_t SECONDS_PER_DAY = SECONDS_PER_HOUR * 24;
const int64_t DAYS_PER_WEEK = 7;

// Leap years and day of the week repeat every 400 years, which makes it
// a good cycle length.
const int64_t SECONDS_PER_400_YEARS =
    SECONDS_PER_DAY * (365 * (300 + 3) + 366 * (100 - 3));

// Is the given year a leap year?
inline bool isLeap(int64_t year) {
  return (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0));
}

// A variant  (eg. PST or PDT) of a timezone (eg. America/Los_Angeles).
struct TimezoneVariant {
  int64_t gmtOffset;
  bool isDst;
  std::string name;

  std::string toString() const { return "Not-implemented"; }
};

// A region that shares the same legal rules for wall clock time and
// day light savings transitions. They are typically named for the largest
// city in the region (eg. America/Los_Angeles or America/Mexico_City).
class Timezone {
 public:
  virtual ~Timezone();

  // Get the variant for the given time (time_t).
  virtual const TimezoneVariant& getVariant(int64_t clk) const = 0;

  // Get the number of seconds between the ORC epoch in this timezone
  // and Unix epoch.
  // ORC epoch is 1 Jan 2015 00:00:00 local.
  // Unix epoch is 1 Jan 1970 00:00:00 UTC.
  virtual int64_t getEpoch() const = 0;

  // Print the timezone to the stream.
  virtual void print(std::ostream&) const = 0;

  // Get the version of the zone file.
  virtual uint64_t getVersion() const = 0;
};

// Get the local timezone.
// Results are cached.
const Timezone& getLocalTimezone();

// Get a timezone by name (eg. America/Los_Angeles).
// Results are cached.
const Timezone& getTimezoneByName(const std::string& zone);

// Parse a set of bytes as a timezone file as if they came from filename.
std::unique_ptr<Timezone> getTimezone(const std::string& filename,
                                      const std::vector<unsigned char>& b);

class TimezoneError : public std::runtime_error {
 public:
  explicit TimezoneError(const std::string& what);
  explicit TimezoneError(const TimezoneError&);
  virtual ~TimezoneError() noexcept;
};

enum TransitionKind { TRANSITION_JULIAN, TRANSITION_DAY, TRANSITION_MONTH };

struct Transition {
  TransitionKind kind;
  int64_t day;
  int64_t week;
  int64_t month;
  int64_t time;

  std::string toString() const {
    std::stringstream buffer;
    switch (kind) {
      case TRANSITION_JULIAN:
        buffer << "julian " << day;
        break;
      case TRANSITION_DAY:
        buffer << "day " << day;
        break;
      case TRANSITION_MONTH:
        buffer << "month " << month << " week " << week << " day " << day;
        break;
    }
    buffer << " at " << (time / (60 * 60)) << ":" << ((time / 60) % 60) << ":"
           << (time % 60);
    return buffer.str();
  }

  // Get the transition time for the given year.
  // @param year the year
  // @return the number of seconds past local Jan 1 00:00:00 that the
  //    transition happens.
  int64_t getTime(int64_t year) const {
    int64_t result = time;
    switch (kind) {
      case TRANSITION_JULIAN:
        result += SECONDS_PER_DAY * day;
        if (day > 60 && isLeap(year)) {
          result += SECONDS_PER_DAY;
        }
        break;
      case TRANSITION_DAY:
        result += SECONDS_PER_DAY * day;
        break;
      case TRANSITION_MONTH: {
        bool inLeap = isLeap(year);
        int64_t adjustedMonth = (month + 9) % 12 + 1;
        int64_t adjustedYear = (month <= 2) ? (year - 1) : year;
        int64_t adjustedCentury = adjustedYear / 100;
        int64_t adjustedRemainder = adjustedYear % 100;

        // day of the week of the first day of month
        int64_t dayOfWeek = ((26 * adjustedMonth - 2) / 10 + 1 +
                             adjustedRemainder + adjustedRemainder / 4 +
                             adjustedCentury / 4 - 2 * adjustedCentury) %
                            7;
        if (dayOfWeek < 0) {
          dayOfWeek += DAYS_PER_WEEK;
        }

        int64_t d = day - dayOfWeek;
        if (d < 0) {
          d += DAYS_PER_WEEK;
        }
        for (int w = 1; w < week; ++w) {
          if (d + DAYS_PER_WEEK >= DAYS_PER_MONTH[inLeap][month - 1]) {
            break;
          }
          d += DAYS_PER_WEEK;
        }
        result += d * SECONDS_PER_DAY;

        // Add in the time for the month
        for (int m = 0; m < month - 1; ++m) {
          result += DAYS_PER_MONTH[inLeap][m] * SECONDS_PER_DAY;
        }
        break;
      }
    }
    return result;
  }
};

// Represents the parsed POSIX timezone rule strings that are used to
// describe the future transitions, because they can go arbitrarily far into
// the future.
class FutureRule {
 public:
  virtual ~FutureRule();
  virtual bool isDefined() const = 0;
  virtual const TimezoneVariant& getVariant(int64_t clk) const = 0;
  virtual void print(std::ostream* out) const = 0;
};

// Parse the POSIX TZ string.
std::unique_ptr<FutureRule> parseFutureRule(const std::string& ruleString);

// The current rule for finding timezone variants arbitrarily far in
// the future.  They are based on a string representation that
// specifies the standard name and offset. For timezones with
// daylight savings, the string specifies the daylight variant name
// and offset and the rules for switching between them.
//
// rule = <standard name><standard offset><daylight>?
// name = string with no numbers or '+', '-', or ','
// offset = [-+]?hh(:mm(:ss)?)?
// daylight = <name><offset>,<start day>(/<offset>)?,<end day>(/<offset>)?
// day = J<day without 2/29>|<day with 2/29>|M<month>.<week>.<day of week>
class FutureRuleImpl : public FutureRule {
  std::string ruleString;
  TimezoneVariant standard;
  bool hasDst;
  TimezoneVariant dst;
  Transition start;
  Transition end;

  // expanded time_t offsets of transitions
  std::vector<int64_t> offsets;

  // Is the epoch (1 Jan 1970 00:00) in standard time?
  // This code assumes that the transition dates fall in the same order
  // each year. Hopefully no timezone regions decide to move across the
  // equator, which is about what it would take.
  bool startInStd;

  void computeOffsets() {
    if (!hasDst) {
      startInStd = true;
      offsets.resize(1);
    } else {
      // Insert a transition for the epoch and two per a year for the next
      // 400 years. We assume that the all even positions are in standard
      // time if and only if startInStd and the odd ones are the reverse.
      offsets.resize(400 * 2 + 1);
      startInStd = start.getTime(1970) < end.getTime(1970);
      int64_t base = 0;
      for (int64_t year = 1970; year < 1970 + 400; ++year) {
        if (startInStd) {
          offsets[static_cast<uint64_t>(year - 1970) * 2 + 1] =
              base + start.getTime(year) - standard.gmtOffset;
          offsets[static_cast<uint64_t>(year - 1970) * 2 + 2] =
              base + end.getTime(year) - dst.gmtOffset;
        } else {
          offsets[static_cast<uint64_t>(year - 1970) * 2 + 1] =
              base + end.getTime(year) - dst.gmtOffset;
          offsets[static_cast<uint64_t>(year - 1970) * 2 + 2] =
              base + start.getTime(year) - standard.gmtOffset;
        }
        base += (isLeap(year) ? 366 : 365) * SECONDS_PER_DAY;
      }
    }
    offsets[0] = 0;
  }

 public:
  virtual ~FutureRuleImpl();
  bool isDefined() const override;
  const TimezoneVariant& getVariant(int64_t clk) const override;
  void print(std::ostream* out) const override;

  friend class FutureRuleParser;
};

// A parser for the future rule strings.
class FutureRuleParser {
 public:
  FutureRuleParser(const std::string& str, FutureRuleImpl* rule)
      : ruleString(str), length(str.size()), position(0), output(*rule) {
    output.ruleString = str;
    if (position != length) {
      parseName(&(output.standard.name));
      output.standard.gmtOffset = -parseOffset();
      output.standard.isDst = false;
      output.hasDst = position < length;
      if (output.hasDst) {
        parseName(&(output.dst.name));
        output.dst.isDst = true;
        if (ruleString[position] != ',') {
          output.dst.gmtOffset = -parseOffset();
        } else {
          output.dst.gmtOffset = output.standard.gmtOffset + 60 * 60;
        }
        parseTransition(&(output.start));
        parseTransition(&(output.end));
      }
      if (position != length) {
        throwError("Extra text");
      }
      output.computeOffsets();
    }
  }

 private:
  const std::string& ruleString;
  size_t length;
  size_t position;
  FutureRuleImpl& output;

  void throwError(const char* msg) {
    std::stringstream buffer;
    buffer << msg << " at " << position << " in '" << ruleString << "'";
    LOG_ERROR(ERRCODE_INTERNAL_ERROR, "%s", buffer.str().c_str());
  }

  // Parse the names of the form:
  //    ([^-+0-9,]+|<[^>]+>)
  // and set the output string.
  void parseName(std::string* result) {
    if (position == length) {
      throwError("name required");
    }
    size_t start = position;
    if (ruleString[position] == '<') {
      while (position < length && ruleString[position] != '>') {
        position += 1;
      }
      if (position == length) {
        throwError("missing close '>'");
      }
      position += 1;
    } else {
      while (position < length) {
        char ch = ruleString[position];
        if (isdigit(ch) || ch == '-' || ch == '+' || ch == ',') {
          break;
        }
        position += 1;
      }
    }
    if (position == start) {
      throwError("empty string not allowed");
    }
    *result = ruleString.substr(start, position - start);
  }

  // Parse an integer of the form [0-9]+ and return it.
  int64_t parseNumber() {
    if (position >= length) {
      throwError("missing number");
    }
    int64_t result = 0;
    while (position < length) {
      char ch = ruleString[position];
      if (isdigit(ch)) {
        result = result * 10 + (ch - '0');
        position += 1;
      } else {
        break;
      }
    }
    return result;
  }

  // Parse the offsets of the form:
  //    [-+]?[0-9]+(:[0-9]+(:[0-9]+)?)?
  // and convert it into a number of seconds.
  int64_t parseOffset() {
    int64_t scale = 3600;
    bool isNegative = false;
    if (position < length) {
      char ch = ruleString[position];
      isNegative = ch == '-';
      if (ch == '-' || ch == '+') {
        position += 1;
      }
    }
    int64_t result = parseNumber() * scale;
    while (position < length && scale > 1 && ruleString[position] == ':') {
      scale /= 60;
      position += 1;
      result += parseNumber() * scale;
    }
    if (isNegative) {
      result = -result;
    }
    return result;
  }

  // Parse a transition of the following form:
  //   ,(J<number>|<number>|M<number>.<number>.<number>)(/<offset>)?
  void parseTransition(Transition* transition) {
    if (length - position < 2 || ruleString[position] != ',') {
      throwError("missing transition");
    }
    position += 1;
    char ch = ruleString[position];
    if (ch == 'J') {
      transition->kind = TRANSITION_JULIAN;
      position += 1;
      transition->day = parseNumber();
    } else if (ch == 'M') {
      transition->kind = TRANSITION_MONTH;
      position += 1;
      transition->month = parseNumber();
      if (position == length || ruleString[position] != '.') {
        throwError("missing first .");
      }
      position += 1;
      transition->week = parseNumber();
      if (position == length || ruleString[position] != '.') {
        throwError("missing second .");
      }
      position += 1;
      transition->day = parseNumber();
    } else {
      transition->kind = TRANSITION_DAY;
      transition->day = parseNumber();
    }
    if (position < length && ruleString[position] == '/') {
      position += 1;
      transition->time = parseOffset();
    } else {
      transition->time = 2 * 60 * 60;
    }
  }
};

// Parse the POSIX TZ string.
extern std::unique_ptr<FutureRule> parseFutureRule(
    const std::string& ruleString);

// An abstraction of the differences between versions.
class VersionParser {
 public:
  virtual ~VersionParser();

  // Get the version number.
  virtual uint64_t getVersion() const = 0;

  // Get the number of bytes
  virtual uint64_t getTimeSize() const = 0;

  // Parse the time at the given location.
  virtual int64_t parseTime(const unsigned char* ptr) const = 0;

  // Parse the future string
  virtual std::string parseFutureString(const unsigned char* ptr,
                                        uint64_t offset,
                                        uint64_t length) const = 0;
};

class TimezoneImpl : public Timezone {
 public:
  TimezoneImpl(const std::string& name, const std::vector<unsigned char> bytes);
  virtual ~TimezoneImpl();

  // Get the variant for the given time (time_t).
  const TimezoneVariant& getVariant(int64_t clk) const override;

  void print(std::ostream&) const override;

  uint64_t getVersion() const override { return version; }

  int64_t getEpoch() const override { return epoch; }

 private:
  void parseTimeVariants(const unsigned char* ptr, uint64_t variantOffset,
                         uint64_t variantCount, uint64_t nameOffset,
                         uint64_t nameCount);
  void parseZoneFile(const unsigned char* ptr, uint64_t sectionOffset,
                     uint64_t fileLength, const VersionParser& version);
  // filename
  std::string filename;

  // the version of the file
  uint64_t version;

  // the list of variants for this timezone
  std::vector<TimezoneVariant> variants;

  // the list of the times where the local rules change
  std::vector<int64_t> transitions;

  // the variant that starts at this transition.
  std::vector<uint64_t> currentVariant;

  // the variant before the first transition
  uint64_t ancientVariant;

  // the rule for future times
  std::unique_ptr<FutureRule> futureRule;

  // the last explicit transition after which we use the future rule
  int64_t lastTransition;

  // The ORC epoch time in this timezone.
  int64_t epoch;
};

class TimezoneUtil {
 public:
  static TimezoneUtil& instance() {
    static TimezoneUtil s;
    return s;
  }

  static int32_t getGMTOffset(std::time_t time) {
    return static_cast<int32_t>(
        instance().timezone_->getVariant(time).gmtOffset);
  }

  static void setGMTOffset(std::string tzname) {
    instance().timezone_ = &(getTimezoneByName(tzname));
  }

 private:
  TimezoneUtil() { timezone_ = nullptr; }
  ~TimezoneUtil() {}
  TimezoneUtil(const TimezoneUtil&) = delete;
  TimezoneUtil& operator=(const TimezoneUtil&) = delete;
  const Timezone* timezone_;
};

}  // namespace dbcommon

#endif  // DBCOMMON_SRC_DBCOMMON_UTILS_TIMEZONE_UTIL_H_
