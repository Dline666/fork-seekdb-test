/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Pure C++ core of the "file as database" feature: streaming CSV/JSONL readers,
// deterministic schema inference and a CSV writer.  This file must not depend on
// any OceanBase header or library; only the C++ standard library and the CRT
// file API are used.  Exceptions are disabled in the server build, therefore no
// throw / try / catch appears here and all failures are reported through int
// return codes plus a std::string error message.

#include "sql/engine/basic/ob_file_format.h"

#include "sql/engine/basic/ob_parquet_format.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <new>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

namespace oceanbase
{
namespace sql
{
namespace file_sql
{

namespace
{

// ---------------------------------------------------------------------------
// Small utilities
// ---------------------------------------------------------------------------

const int64_t FSQL_STREAM_BUFFER_SIZE = 64 * 1024;

// Bits used while a value is classified.  A single value always maps to exactly
// one bit; a column accumulates the OR of the bits of all its non-NULL values.
enum FsqlValueBit : int32_t
{
  FSQL_BIT_INT = 1,
  FSQL_BIT_DOUBLE = 2,
  FSQL_BIT_BOOL = 4,
  FSQL_BIT_DATE = 8,
  FSQL_BIT_DATETIME = 16,
  FSQL_BIT_TEXT = 32,
};

inline bool fsql_is_digit_char(int c) { return c >= '0' && c <= '9'; }

inline bool fsql_is_json_space(int c)
{
  return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

inline char fsql_lower_char(char c)
{
  return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
}

inline bool fsql_ascii_iequal_literal(const std::string &value, const char *literal)
{
  if (literal == nullptr) {
    return false;
  }
  const size_t len = std::strlen(literal);
  if (value.size() != len) {
    return false;
  }
  for (size_t i = 0; i < len; ++i) {
    if (fsql_lower_char(value[i]) != literal[i]) {
      return false;
    }
  }
  return true;
}

inline bool fsql_ascii_iequal_str(const std::string &a, const std::string &b)
{
  if (a.size() != b.size()) {
    return false;
  }
  for (size_t i = 0; i < a.size(); ++i) {
    if (fsql_lower_char(a[i]) != fsql_lower_char(b[i])) {
      return false;
    }
  }
  return true;
}

// Renders a double with the shortest representation that round-trips.  Integral
// values keep a plain integer form (90 -> "90"), everything else uses the
// shortest %g form that parses back to the very same double.
inline std::string fsql_double_to_text(double value)
{
  char buffer[64];
  if (std::isnan(value)) {
    return std::string("nan");
  }
  if (std::isinf(value)) {
    return std::string(value < 0 ? "-inf" : "inf");
  }
  if (value == 0.0) {
    return std::string(std::signbit(value) ? "-0" : "0");
  }
  if (std::fabs(value) < 1e15 && std::floor(value) == value) {
    std::snprintf(buffer, sizeof(buffer), "%.0f", value);
    return std::string(buffer);
  }
  for (int precision = 1; precision <= 17; ++precision) {
    std::snprintf(buffer, sizeof(buffer), "%.*g", precision, value);
    char *end = nullptr;
    errno = 0;
    const double parsed = std::strtod(buffer, &end);
    if (end != nullptr && *end == '\0' && parsed == value) {
      return std::string(buffer);
    }
  }
  std::snprintf(buffer, sizeof(buffer), "%.17g", value);
  return std::string(buffer);
}

inline void fsql_append_utf8(std::string &out, uint32_t codepoint)
{
  if (codepoint <= 0x7F) {
    out.push_back(static_cast<char>(codepoint));
  } else if (codepoint <= 0x7FF) {
    out.push_back(static_cast<char>(0xC0 | (codepoint >> 6)));
    out.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
  } else if (codepoint <= 0xFFFF) {
    out.push_back(static_cast<char>(0xE0 | (codepoint >> 12)));
    out.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
    out.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
  } else if (codepoint <= 0x10FFFF) {
    out.push_back(static_cast<char>(0xF0 | (codepoint >> 18)));
    out.push_back(static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F)));
    out.push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
    out.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
  } else {
    out.push_back('?');
  }
}

// ---------------------------------------------------------------------------
// Calendar validation
// ---------------------------------------------------------------------------

struct FsqlDateParts
{
  int32_t year = 0;
  int32_t month = 0;
  int32_t day = 0;
};

struct FsqlTimeParts
{
  int32_t hour = 0;
  int32_t minute = 0;
  int32_t second = 0;
  int32_t micro = 0;
};

struct FsqlDateTimeParts
{
  FsqlDateParts date;
  FsqlTimeParts time;
};

inline bool fsql_is_leap_year(int32_t year)
{
  return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

inline int32_t fsql_month_days(int32_t year, int32_t month)
{
  static const int32_t k_days[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (month < 1 || month > 12) {
    return 0;
  }
  if (month == 2 && fsql_is_leap_year(year)) {
    return 29;
  }
  return k_days[month - 1];
}

inline bool fsql_valid_date_parts(const FsqlDateParts &date)
{
  if (date.month < 1 || date.month > 12) {
    return false;
  }
  const int32_t month_days = fsql_month_days(date.year, date.month);
  return date.day >= 1 && date.day <= month_days;
}

inline bool fsql_valid_time_parts(const FsqlTimeParts &time)
{
  return time.hour >= 0 && time.hour <= 23 && time.minute >= 0 && time.minute <= 59 &&
         time.second >= 0 && time.second <= 59 && time.micro >= 0 && time.micro <= 999999;
}

inline bool fsql_read_digits(const char *text, size_t len, size_t &pos, size_t count, int32_t &out)
{
  if (pos + count > len) {
    return false;
  }
  int32_t value = 0;
  for (size_t i = 0; i < count; ++i) {
    const char c = text[pos + i];
    if (!fsql_is_digit_char(c)) {
      return false;
    }
    value = value * 10 + static_cast<int32_t>(c - '0');
  }
  pos += count;
  out = value;
  return true;
}

inline bool fsql_parse_date_text(const char *text, size_t len, FsqlDateParts &out)
{
  if (text == nullptr || len != 10) {
    return false;
  }
  size_t pos = 0;
  FsqlDateParts date;
  if (!fsql_read_digits(text, len, pos, 4, date.year) || pos >= len || text[pos] != '-') {
    return false;
  }
  ++pos;
  if (!fsql_read_digits(text, len, pos, 2, date.month) || pos >= len || text[pos] != '-') {
    return false;
  }
  ++pos;
  if (!fsql_read_digits(text, len, pos, 2, date.day) || pos != len) {
    return false;
  }
  if (!fsql_valid_date_parts(date)) {
    return false;
  }
  out = date;
  return true;
}

inline bool fsql_parse_time_text(const char *text, size_t len, size_t &pos, FsqlTimeParts &out)
{
  FsqlTimeParts time;
  if (!fsql_read_digits(text, len, pos, 2, time.hour) || pos >= len || text[pos] != ':') {
    return false;
  }
  ++pos;
  if (!fsql_read_digits(text, len, pos, 2, time.minute) || pos >= len || text[pos] != ':') {
    return false;
  }
  ++pos;
  if (!fsql_read_digits(text, len, pos, 2, time.second)) {
    return false;
  }
  if (pos < len && text[pos] == '.') {
    ++pos;
    int32_t fraction = 0;
    size_t digits = 0;
    while (pos < len && fsql_is_digit_char(text[pos]) && digits < 6) {
      fraction = fraction * 10 + static_cast<int32_t>(text[pos] - '0');
      ++pos;
      ++digits;
    }
    if (digits == 0) {
      return false;
    }
    while (digits < 6) {
      fraction *= 10;
      ++digits;
    }
    time.micro = fraction;
  }
  if (!fsql_valid_time_parts(time)) {
    return false;
  }
  out = time;
  return true;
}

inline bool fsql_parse_datetime_text(const char *text, size_t len, FsqlDateTimeParts &out)
{
  if (text == nullptr || len < 19) {
    return false;
  }
  FsqlDateParts date;
  if (!fsql_parse_date_text(text, 10, date)) {
    return false;
  }
  if (text[10] != ' ' && text[10] != 'T') {
    return false;
  }
  size_t pos = 11;
  FsqlTimeParts time;
  if (!fsql_parse_time_text(text, len, pos, time) || pos != len) {
    return false;
  }
  out.date = date;
  out.time = time;
  return true;
}

inline void fsql_format_date_text(const FsqlDateParts &date, std::string &out)
{
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d", date.year, date.month, date.day);
  out.assign(buffer);
}

inline void fsql_format_datetime_text(const FsqlDateTimeParts &datetime, std::string &out)
{
  char buffer[128];
  if (datetime.time.micro == 0) {
    std::snprintf(buffer,
                  sizeof(buffer),
                  "%04d-%02d-%02d %02d:%02d:%02d",
                  datetime.date.year,
                  datetime.date.month,
                  datetime.date.day,
                  datetime.time.hour,
                  datetime.time.minute,
                  datetime.time.second);
  } else {
    int32_t fraction = datetime.time.micro;
    int32_t width = 6;
    while (width > 0 && fraction % 10 == 0) {
      fraction /= 10;
      --width;
    }
    std::snprintf(buffer,
                  sizeof(buffer),
                  "%04d-%02d-%02d %02d:%02d:%02d.%0*d",
                  datetime.date.year,
                  datetime.date.month,
                  datetime.date.day,
                  datetime.time.hour,
                  datetime.time.minute,
                  datetime.time.second,
                  static_cast<int>(width),
                  static_cast<int>(fraction));
  }
  out.assign(buffer);
}

// ---------------------------------------------------------------------------
// Scalar text parsing
// ---------------------------------------------------------------------------

inline bool fsql_parse_int64_text(const char *text, size_t len, int64_t &out)
{
  if (text == nullptr || len == 0) {
    return false;
  }
  size_t pos = 0;
  bool negative = false;
  if (text[pos] == '+' || text[pos] == '-') {
    negative = text[pos] == '-';
    ++pos;
  }
  if (pos >= len) {
    return false;
  }
  const uint64_t limit =
      negative ? static_cast<uint64_t>(9223372036854775808ULL) : static_cast<uint64_t>(9223372036854775807ULL);
  uint64_t value = 0;
  for (; pos < len; ++pos) {
    const char c = text[pos];
    if (!fsql_is_digit_char(c)) {
      return false;
    }
    const uint64_t digit = static_cast<uint64_t>(c - '0');
    if (value > (limit - digit) / 10) {
      return false;
    }
    value = value * 10 + digit;
  }
  if (negative) {
    if (value == 9223372036854775808ULL) {
      out = -9223372036854775807LL - 1;
    } else {
      out = -static_cast<int64_t>(value);
    }
  } else {
    out = static_cast<int64_t>(value);
  }
  return true;
}

inline bool fsql_validate_decimal_syntax(const char *text, size_t len)
{
  if (text == nullptr || len == 0) {
    return false;
  }
  size_t pos = 0;
  if (text[pos] == '+' || text[pos] == '-') {
    ++pos;
  }
  size_t mantissa_digits = 0;
  while (pos < len && fsql_is_digit_char(text[pos])) {
    ++pos;
    ++mantissa_digits;
  }
  if (pos < len && text[pos] == '.') {
    ++pos;
    while (pos < len && fsql_is_digit_char(text[pos])) {
      ++pos;
      ++mantissa_digits;
    }
  }
  if (mantissa_digits == 0) {
    return false;
  }
  if (pos < len && (text[pos] == 'e' || text[pos] == 'E')) {
    ++pos;
    if (pos < len && (text[pos] == '+' || text[pos] == '-')) {
      ++pos;
    }
    size_t exponent_digits = 0;
    while (pos < len && fsql_is_digit_char(text[pos])) {
      ++pos;
      ++exponent_digits;
    }
    if (exponent_digits == 0) {
      return false;
    }
  }
  return pos == len;
}

inline bool fsql_parse_double_text(const char *text, size_t len, double &out)
{
  if (!fsql_validate_decimal_syntax(text, len)) {
    return false;
  }
  char stack_buffer[64];
  char *buffer = stack_buffer;
  std::string heap_buffer;
  if (len + 1 > sizeof(stack_buffer)) {
    heap_buffer.assign(text, len);
    heap_buffer.push_back('\0');
    buffer = &heap_buffer[0];
  } else {
    std::memcpy(stack_buffer, text, len);
    stack_buffer[len] = '\0';
  }
  errno = 0;
  char *end = nullptr;
  const double value = std::strtod(buffer, &end);
  if (end != buffer + len) {
    return false;
  }
  if (!std::isfinite(value)) {
    return false;
  }
  out = value;
  return true;
}

// Returns 1 for a true literal, 0 for a false literal and -1 when the text is
// not a boolean literal at all.
inline int fsql_parse_bool_text(const char *text, size_t len)
{
  static const char *k_literals[] = {"true", "t", "yes", "y", "false", "f", "no", "n", "1", "0"};
  static const int k_values[] = {1, 1, 1, 1, 0, 0, 0, 0, 1, 0};
  if (text == nullptr || len == 0) {
    return -1;
  }
  for (size_t i = 0; i < sizeof(k_literals) / sizeof(k_literals[0]); ++i) {
    const char *literal = k_literals[i];
    const size_t literal_len = std::strlen(literal);
    if (literal_len != len) {
      continue;
    }
    bool same = true;
    for (size_t j = 0; j < len; ++j) {
      if (fsql_lower_char(text[j]) != literal[j]) {
        same = false;
        break;
      }
    }
    if (same) {
      return k_values[i];
    }
  }
  return -1;
}

// Classifies one text value.  The order matters: "1" is an integer, not a
// boolean, so a column holding only 0/1 becomes BIGINT.
inline int32_t fsql_classify_text_value(const char *text, size_t len)
{
  int64_t int_value = 0;
  if (fsql_parse_int64_text(text, len, int_value)) {
    return FSQL_BIT_INT;
  }
  double double_value = 0;
  if (fsql_parse_double_text(text, len, double_value)) {
    return FSQL_BIT_DOUBLE;
  }
  if (fsql_parse_bool_text(text, len) >= 0) {
    return FSQL_BIT_BOOL;
  }
  FsqlDateParts date;
  if (fsql_parse_date_text(text, len, date)) {
    return FSQL_BIT_DATE;
  }
  FsqlDateTimeParts datetime;
  if (fsql_parse_datetime_text(text, len, datetime)) {
    return FSQL_BIT_DATETIME;
  }
  return FSQL_BIT_TEXT;
}

// A JSON string only ever becomes DATE/DATETIME/VARCHAR; numbers and booleans
// inside a string are not reinterpreted.
inline int32_t fsql_classify_json_string(const std::string &value)
{
  FsqlDateParts date;
  if (fsql_parse_date_text(value.data(), value.size(), date)) {
    return FSQL_BIT_DATE;
  }
  FsqlDateTimeParts datetime;
  if (fsql_parse_datetime_text(value.data(), value.size(), datetime)) {
    return FSQL_BIT_DATETIME;
  }
  return FSQL_BIT_TEXT;
}

// CSV widening: a column that mixes bigint and 0/1/true/false literals becomes
// BIGINT (any DOUBLE wins over BIGINT), dates and datetimes widen to DATETIME
// and anything mixed with text or with numbers becomes VARCHAR.
inline FileColumnType fsql_mask_to_column_type(int32_t mask)
{
  const int32_t date_mask = FSQL_BIT_DATE | FSQL_BIT_DATETIME;
  if ((mask & date_mask) != 0) {
    if ((mask & ~date_mask) != 0) {
      return FILE_COL_VARCHAR;
    }
    return (mask & FSQL_BIT_DATETIME) != 0 ? FILE_COL_DATETIME : FILE_COL_DATE;
  }
  if ((mask & FSQL_BIT_TEXT) != 0) {
    return FILE_COL_VARCHAR;
  }
  if ((mask & FSQL_BIT_DOUBLE) != 0) {
    return FILE_COL_DOUBLE;
  }
  if ((mask & FSQL_BIT_INT) != 0) {
    return FILE_COL_BIGINT;
  }
  if ((mask & FSQL_BIT_BOOL) != 0) {
    return FILE_COL_BOOLEAN;
  }
  return FILE_COL_NULL;
}

// JSON widening: only BIGINT+DOUBLE is a compatible mix; every other mix (a
// boolean with a number, a string with a number, a date with a number) degrades
// to VARCHAR instead of failing.
inline FileColumnType fsql_json_mask_to_column_type(int32_t mask)
{
  const int32_t date_mask = FSQL_BIT_DATE | FSQL_BIT_DATETIME;
  const int32_t number_mask = FSQL_BIT_INT | FSQL_BIT_DOUBLE;
  if (mask == 0) {
    return FILE_COL_NULL;
  }
  if ((mask & FSQL_BIT_TEXT) != 0) {
    return FILE_COL_VARCHAR;
  }
  if ((mask & date_mask) != 0) {
    if ((mask & ~date_mask) != 0) {
      return FILE_COL_VARCHAR;
    }
    return (mask & FSQL_BIT_DATETIME) != 0 ? FILE_COL_DATETIME : FILE_COL_DATE;
  }
  if ((mask & FSQL_BIT_BOOL) != 0 && (mask & number_mask) != 0) {
    return FILE_COL_VARCHAR;
  }
  if ((mask & FSQL_BIT_DOUBLE) != 0) {
    return FILE_COL_DOUBLE;
  }
  if ((mask & FSQL_BIT_INT) != 0) {
    return FILE_COL_BIGINT;
  }
  return FILE_COL_BOOLEAN;
}

// ---------------------------------------------------------------------------
// Buffered streaming file input
// ---------------------------------------------------------------------------

class FsqlFileStream
{
public:
  FsqlFileStream() : fp_(nullptr), pos_(0), len_(0), at_eof_(false), pushback_(-1), line_(1) {}
  ~FsqlFileStream() { close(); }

  FsqlFileStream(const FsqlFileStream &) = delete;
  FsqlFileStream &operator=(const FsqlFileStream &) = delete;

  int open(const std::string &path)
  {
    close();
    fp_ = std::fopen(path.c_str(), "rb");
    if (fp_ == nullptr) {
      return -1;
    }
    buffer_.resize(static_cast<size_t>(FSQL_STREAM_BUFFER_SIZE));
    // Skip a UTF-8 BOM when present; the handle is brand new so rewinding is safe.
    char bom[3];
    const size_t read_bytes = std::fread(bom, 1, sizeof(bom), fp_);
    const bool has_bom = read_bytes == 3 && static_cast<unsigned char>(bom[0]) == 0xEF &&
                         static_cast<unsigned char>(bom[1]) == 0xBB &&
                         static_cast<unsigned char>(bom[2]) == 0xBF;
    if (std::fseek(fp_, has_bom ? 3 : 0, SEEK_SET) != 0) {
      // Non seekable input: not supported, the caller sees a read failure later.
      std::clearerr(fp_);
    }
    return 0;
  }

  void close()
  {
    if (fp_ != nullptr) {
      std::fclose(fp_);
      fp_ = nullptr;
    }
    pos_ = 0;
    len_ = 0;
    at_eof_ = false;
    pushback_ = -1;
    line_ = 1;
  }

  int next()
  {
    if (pushback_ >= 0) {
      const int c = pushback_;
      pushback_ = -1;
      return c;
    }
    return raw_get();
  }

  int peek()
  {
    const int c = next();
    if (c >= 0) {
      pushback_ = c;
    }
    return c;
  }

  int64_t line() const { return line_; }

private:
  int raw_get()
  {
    if (pos_ < len_) {
      const int c = static_cast<int>(static_cast<unsigned char>(buffer_[pos_++]));
      if (c == '\n') {
        ++line_;
      }
      return c;
    }
    if (at_eof_ || fp_ == nullptr) {
      return -1;
    }
    const size_t read_bytes = std::fread(&buffer_[0], 1, buffer_.size(), fp_);
    if (read_bytes == 0) {
      at_eof_ = true;
      return -1;
    }
    len_ = read_bytes;
    pos_ = 0;
    const int c = static_cast<int>(static_cast<unsigned char>(buffer_[pos_++]));
    if (c == '\n') {
      ++line_;
    }
    return c;
  }

  std::FILE *fp_;
  std::vector<char> buffer_;
  size_t pos_;
  size_t len_;
  bool at_eof_;
  int pushback_;
  int64_t line_;
};

// ---------------------------------------------------------------------------
// CSV tokenizer
// ---------------------------------------------------------------------------

struct FsqlCsvField
{
  std::string text;
  bool quoted = false;
};

inline bool fsql_csv_field_is_null(const FsqlCsvField &field, const std::string &null_str)
{
  if (field.quoted) {
    return false;
  }
  if (field.text.empty()) {
    return true;
  }
  return !null_str.empty() && field.text == null_str;
}

class FsqlCsvParser
{
public:
  FsqlCsvParser() : stream_(nullptr), delimiter_(','), quote_('"'), escape_('"'), record_line_(1) {}

  void open(FsqlFileStream &stream, const FileScanOptions &options)
  {
    stream_ = &stream;
    delimiter_ = options.delimiter;
    quote_ = options.quote;
    escape_ = options.escape;
  }

  // Returns 1 when a record was read, 0 at end of file and -1 on error.
  int read_record(std::vector<FsqlCsvField> &fields, std::string &err)
  {
    fields.clear();
    if (stream_ == nullptr) {
      err = "csv parser is not initialized";
      return -1;
    }
    const int delimiter_ch = static_cast<int>(static_cast<unsigned char>(delimiter_));
    const int quote_ch = static_cast<int>(static_cast<unsigned char>(quote_));
    // Blank lines are not records.
    for (;;) {
      int c = stream_->peek();
      if (c < 0) {
        return 0;
      }
      if (c == '\n') {
        stream_->next();
        continue;
      }
      if (c == '\r') {
        stream_->next();
        if (stream_->peek() == '\n') {
          stream_->next();
        }
        continue;
      }
      break;
    }
    record_line_ = stream_->line();
    for (;;) {
      FsqlCsvField field;
      bool end_of_record = false;
      const int c = stream_->peek();
      if (c == quote_ch) {
        field.quoted = true;
        stream_->next();
        if (!parse_quoted_field(field.text, err)) {
          return -1;
        }
        // Anything between the closing quote and the delimiter is ignored.
        for (;;) {
          const int look = stream_->peek();
          if (look < 0 || look == delimiter_ch || look == '\n') {
            break;
          }
          if (look == '\r') {
            stream_->next();
            if (stream_->peek() == '\n') {
              stream_->next();
            }
            end_of_record = true;
            break;
          }
          stream_->next();
        }
      } else if (c >= 0) {
        parse_unquoted_field(field.text, end_of_record);
      }
      fields.push_back(field);
      if (end_of_record) {
        return 1;
      }
      const int terminator = stream_->peek();
      if (terminator < 0) {
        return 1;
      }
      if (terminator == delimiter_ch) {
        stream_->next();
        continue;
      }
      if (terminator == '\n') {
        stream_->next();
        return 1;
      }
      if (terminator == '\r') {
        stream_->next();
        if (stream_->peek() == '\n') {
          stream_->next();
        }
        return 1;
      }
      // Defensive: cannot happen because the field parsers stop on those bytes.
      stream_->next();
      return 1;
    }
  }

private:
  bool parse_quoted_field(std::string &text, std::string &err)
  {
    const int quote_ch = static_cast<int>(static_cast<unsigned char>(quote_));
    const int escape_ch = static_cast<int>(static_cast<unsigned char>(escape_));
    for (;;) {
      const int c = stream_->next();
      if (c < 0) {
        err = "unterminated quoted field starting at line " + std::to_string(record_line_);
        return false;
      }
      if (c == quote_ch) {
        const int look = stream_->peek();
        if (look == quote_ch) {
          text.push_back(quote_);
          stream_->next();
          continue;
        }
        return true;
      }
      if (escape_ch != quote_ch && c == escape_ch) {
        const int escaped = stream_->next();
        if (escaped < 0) {
          text.push_back(escape_);
          return true;
        }
        text.push_back(static_cast<char>(escaped));
        continue;
      }
      if (c == '\r') {
        // Inside quotes the newline is literal data; CRLF is normalized to LF so
        // that the same record read from a CRLF and a LF file compares equal.
        const int look = stream_->peek();
        if (look == '\n') {
          stream_->next();
          text.push_back('\n');
        } else {
          text.push_back('\r');
        }
        continue;
      }
      text.push_back(static_cast<char>(c));
    }
  }

  void parse_unquoted_field(std::string &text, bool &end_of_record)
  {
    const int delimiter_ch = static_cast<int>(static_cast<unsigned char>(delimiter_));
    end_of_record = false;
    for (;;) {
      const int c = stream_->peek();
      if (c < 0) {
        break;
      }
      if (c == delimiter_ch) {
        break;
      }
      if (c == '\n') {
        stream_->next();
        end_of_record = true;
        break;
      }
      if (c == '\r') {
        stream_->next();
        if (stream_->peek() == '\n') {
          stream_->next();
          end_of_record = true;
          break;
        }
        // A CR that is not followed by LF is data.
        text.push_back('\r');
        continue;
      }
      stream_->next();
      text.push_back(static_cast<char>(c));
    }
  }

  FsqlFileStream *stream_;
  char delimiter_;
  char quote_;
  char escape_;
  int64_t record_line_;
};

// ---------------------------------------------------------------------------
// Header name handling
// ---------------------------------------------------------------------------

inline std::string fsql_default_column_name(size_t index)
{
  return std::string("column") + std::to_string(index + 1);
}

inline bool fsql_column_name_in_use(const std::vector<std::string> &names, const std::string &candidate)
{
  for (size_t i = 0; i < names.size(); ++i) {
    if (fsql_ascii_iequal_str(names[i], candidate)) {
      return true;
    }
  }
  return false;
}

// The raw header text is kept verbatim; empty names fall back to "columnN" and
// case insensitive duplicates get a "__N" suffix.
inline std::string fsql_make_unique_column_name(const std::vector<std::string> &names,
                                                const std::string &raw,
                                                size_t index)
{
  const std::string base = raw.empty() ? fsql_default_column_name(index) : raw;
  std::string candidate = base;
  int64_t suffix = 2;
  while (fsql_column_name_in_use(names, candidate)) {
    candidate = base + "__" + std::to_string(suffix);
    ++suffix;
    if (suffix > 1000000) {
      candidate = base + "_" + std::to_string(index + 1);
      break;
    }
  }
  return candidate;
}

// ---------------------------------------------------------------------------
// CSV / JSON cell construction
// ---------------------------------------------------------------------------

inline void fsql_reset_cell(Cell &cell, FileColumnType type)
{
  cell.is_null = true;
  cell.type = type;
  cell.int_value = 0;
  cell.double_value = 0;
  cell.bool_value = false;
  cell.str_value.clear();
}

inline void fsql_csv_field_to_cell(const FsqlCsvField &field,
                                   const std::string &null_str,
                                   FileColumnType type,
                                   Cell &cell)
{
  fsql_reset_cell(cell, type);
  if (fsql_csv_field_is_null(field, null_str)) {
    return;
  }
  cell.is_null = false;
  const char *text = field.text.data();
  const size_t len = field.text.size();
  switch (type) {
    case FILE_COL_BIGINT: {
      int64_t int_value = 0;
      if (fsql_parse_int64_text(text, len, int_value)) {
        cell.int_value = int_value;
      } else {
        // A column that mixed bigint and 0/1/true/false literals is typed BIGINT.
        const int bool_value = fsql_parse_bool_text(text, len);
        if (bool_value >= 0) {
          cell.int_value = bool_value;
          cell.bool_value = bool_value != 0;
        } else {
          cell.is_null = true;
        }
      }
      break;
    }
    case FILE_COL_DOUBLE: {
      double double_value = 0;
      if (fsql_parse_double_text(text, len, double_value)) {
        cell.double_value = double_value;
      } else {
        const int bool_value = fsql_parse_bool_text(text, len);
        if (bool_value >= 0) {
          cell.double_value = static_cast<double>(bool_value);
        } else {
          cell.is_null = true;
        }
      }
      break;
    }
    case FILE_COL_BOOLEAN: {
      const int bool_value = fsql_parse_bool_text(text, len);
      if (bool_value >= 0) {
        cell.bool_value = bool_value != 0;
        cell.int_value = bool_value;
      } else {
        int64_t int_value = 0;
        double double_value = 0;
        if (fsql_parse_int64_text(text, len, int_value)) {
          cell.int_value = int_value;
          cell.bool_value = int_value != 0;
        } else if (fsql_parse_double_text(text, len, double_value)) {
          cell.bool_value = double_value != 0;
          cell.int_value = cell.bool_value ? 1 : 0;
        } else {
          cell.is_null = true;
        }
      }
      break;
    }
    case FILE_COL_DATE: {
      FsqlDateParts date;
      if (fsql_parse_date_text(text, len, date)) {
        fsql_format_date_text(date, cell.str_value);
      } else {
        FsqlDateTimeParts datetime;
        if (fsql_parse_datetime_text(text, len, datetime)) {
          fsql_format_date_text(datetime.date, cell.str_value);
        } else {
          cell.is_null = true;
        }
      }
      break;
    }
    case FILE_COL_DATETIME: {
      FsqlDateTimeParts datetime;
      if (fsql_parse_datetime_text(text, len, datetime)) {
        fsql_format_datetime_text(datetime, cell.str_value);
      } else {
        FsqlDateParts date;
        if (fsql_parse_date_text(text, len, date)) {
          datetime.date = date;
          fsql_format_datetime_text(datetime, cell.str_value);
        } else {
          cell.is_null = true;
        }
      }
      break;
    }
    case FILE_COL_VARCHAR:
    case FILE_COL_NULL:
    default:
      cell.str_value = field.text;
      break;
  }
}

enum FsqlJsonKind : int32_t
{
  FSQL_JSON_NULL = 0,
  FSQL_JSON_BOOL = 1,
  FSQL_JSON_INT = 2,
  FSQL_JSON_DOUBLE = 3,
  FSQL_JSON_STRING = 4,
};

struct FsqlJsonValue
{
  int32_t kind = FSQL_JSON_NULL;
  bool bool_value = false;
  int64_t int_value = 0;
  double double_value = 0;
  std::string str_value;
};

using FsqlJsonObject = std::vector<std::pair<std::string, FsqlJsonValue>>;

inline int32_t fsql_json_value_bits(const FsqlJsonValue &value)
{
  switch (value.kind) {
    case FSQL_JSON_BOOL:
      return FSQL_BIT_BOOL;
    case FSQL_JSON_INT:
      return FSQL_BIT_INT;
    case FSQL_JSON_DOUBLE:
      return FSQL_BIT_DOUBLE;
    case FSQL_JSON_STRING:
      return fsql_classify_json_string(value.str_value);
    case FSQL_JSON_NULL:
    default:
      return 0;
  }
}

inline void fsql_json_value_to_text(const FsqlJsonValue &value, std::string &out)
{
  out.clear();
  switch (value.kind) {
    case FSQL_JSON_BOOL:
      out = value.bool_value ? "true" : "false";
      break;
    case FSQL_JSON_INT:
      out = std::to_string(value.int_value);
      break;
    case FSQL_JSON_DOUBLE:
      out = fsql_double_to_text(value.double_value);
      break;
    case FSQL_JSON_STRING:
      out = value.str_value;
      break;
    case FSQL_JSON_NULL:
    default:
      break;
  }
}

inline void fsql_json_value_to_cell(const FsqlJsonValue &value, FileColumnType type, Cell &cell)
{
  fsql_reset_cell(cell, type);
  if (value.kind == FSQL_JSON_NULL) {
    return;
  }
  cell.is_null = false;
  switch (type) {
    case FILE_COL_BIGINT: {
      if (value.kind == FSQL_JSON_INT) {
        cell.int_value = value.int_value;
      } else if (value.kind == FSQL_JSON_DOUBLE) {
        cell.int_value = static_cast<int64_t>(value.double_value);
      } else if (value.kind == FSQL_JSON_BOOL) {
        cell.int_value = value.bool_value ? 1 : 0;
        cell.bool_value = value.bool_value;
      } else if (value.kind == FSQL_JSON_STRING) {
        int64_t int_value = 0;
        if (fsql_parse_int64_text(value.str_value.data(), value.str_value.size(), int_value)) {
          cell.int_value = int_value;
        } else {
          cell.is_null = true;
        }
      } else {
        cell.is_null = true;
      }
      break;
    }
    case FILE_COL_DOUBLE: {
      if (value.kind == FSQL_JSON_DOUBLE) {
        cell.double_value = value.double_value;
      } else if (value.kind == FSQL_JSON_INT) {
        cell.double_value = static_cast<double>(value.int_value);
      } else if (value.kind == FSQL_JSON_BOOL) {
        cell.double_value = value.bool_value ? 1.0 : 0.0;
      } else if (value.kind == FSQL_JSON_STRING) {
        double double_value = 0;
        if (fsql_parse_double_text(value.str_value.data(), value.str_value.size(), double_value)) {
          cell.double_value = double_value;
        } else {
          cell.is_null = true;
        }
      } else {
        cell.is_null = true;
      }
      break;
    }
    case FILE_COL_BOOLEAN: {
      if (value.kind == FSQL_JSON_BOOL) {
        cell.bool_value = value.bool_value;
        cell.int_value = value.bool_value ? 1 : 0;
      } else if (value.kind == FSQL_JSON_INT) {
        cell.bool_value = value.int_value != 0;
        cell.int_value = cell.bool_value ? 1 : 0;
      } else if (value.kind == FSQL_JSON_DOUBLE) {
        cell.bool_value = value.double_value != 0.0;
        cell.int_value = cell.bool_value ? 1 : 0;
      } else if (value.kind == FSQL_JSON_STRING) {
        const int bool_value =
            fsql_parse_bool_text(value.str_value.data(), value.str_value.size());
        if (bool_value >= 0) {
          cell.bool_value = bool_value != 0;
          cell.int_value = bool_value;
        } else {
          cell.is_null = true;
        }
      } else {
        cell.is_null = true;
      }
      break;
    }
    case FILE_COL_DATE: {
      FsqlDateParts date;
      if (value.kind == FSQL_JSON_STRING &&
          fsql_parse_date_text(value.str_value.data(), value.str_value.size(), date)) {
        fsql_format_date_text(date, cell.str_value);
      } else {
        cell.is_null = true;
      }
      break;
    }
    case FILE_COL_DATETIME: {
      FsqlDateTimeParts datetime;
      if (value.kind == FSQL_JSON_STRING &&
          fsql_parse_datetime_text(value.str_value.data(), value.str_value.size(), datetime)) {
        fsql_format_datetime_text(datetime, cell.str_value);
      } else if (value.kind == FSQL_JSON_STRING) {
        FsqlDateParts date;
        if (fsql_parse_date_text(value.str_value.data(), value.str_value.size(), date)) {
          datetime.date = date;
          fsql_format_datetime_text(datetime, cell.str_value);
        } else {
          cell.is_null = true;
        }
      } else {
        cell.is_null = true;
      }
      break;
    }
    case FILE_COL_VARCHAR:
    case FILE_COL_NULL:
    default:
      fsql_json_value_to_text(value, cell.str_value);
      break;
  }
}

inline const FsqlJsonValue *fsql_json_find_field(const FsqlJsonObject &object, const std::string &key)
{
  for (size_t i = 0; i < object.size(); ++i) {
    if (object[i].first == key) {
      return &object[i].second;
    }
  }
  return nullptr;
}

// ---------------------------------------------------------------------------
// JSON tokenizer
// ---------------------------------------------------------------------------

class FsqlJsonCursor
{
public:
  FsqlJsonCursor() : stream_(nullptr) {}

  void init(FsqlFileStream *stream) { stream_ = stream; }

  int peek() { return stream_->peek(); }
  int get() { return stream_->next(); }
  int64_t line() const { return stream_->line(); }

  void skip_ws()
  {
    for (;;) {
      const int c = stream_->peek();
      if (!fsql_is_json_space(c)) {
        break;
      }
      stream_->next();
    }
  }

  bool parse_string(std::string &out, std::string &err)
  {
    out.clear();
    if (stream_->peek() != '"') {
      err = "expected a JSON string";
      return false;
    }
    stream_->next();
    for (;;) {
      const int c = stream_->next();
      if (c < 0) {
        err = "unterminated JSON string";
        return false;
      }
      if (c == '"') {
        return true;
      }
      if (c != '\\') {
        out.push_back(static_cast<char>(c));
        continue;
      }
      const int escaped = stream_->next();
      switch (escaped) {
        case '"':
          out.push_back('"');
          break;
        case '\\':
          out.push_back('\\');
          break;
        case '/':
          out.push_back('/');
          break;
        case 'b':
          out.push_back('\b');
          break;
        case 'f':
          out.push_back('\f');
          break;
        case 'n':
          out.push_back('\n');
          break;
        case 'r':
          out.push_back('\r');
          break;
        case 't':
          out.push_back('\t');
          break;
        case 'u': {
          uint32_t codepoint = 0;
          if (!read_hex4(codepoint)) {
            err = "invalid \\u escape in JSON string";
            return false;
          }
          if (codepoint >= 0xD800 && codepoint <= 0xDBFF) {
            const int first = stream_->next();
            if (first == '\\' && stream_->next() == 'u') {
              uint32_t low = 0;
              if (read_hex4(low) && low >= 0xDC00 && low <= 0xDFFF) {
                codepoint = 0x10000 + ((codepoint - 0xD800) << 10) + (low - 0xDC00);
              } else {
                codepoint = 0xFFFD;
              }
            } else {
              codepoint = 0xFFFD;
            }
          } else if (codepoint >= 0xDC00 && codepoint <= 0xDFFF) {
            codepoint = 0xFFFD;
          }
          fsql_append_utf8(out, codepoint);
          break;
        }
        default:
          err = "invalid escape sequence in JSON string";
          return false;
      }
    }
  }

  bool parse_literal_token(std::string &out, std::string &err)
  {
    out.clear();
    for (;;) {
      const int c = stream_->peek();
      if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
        out.push_back(static_cast<char>(stream_->next()));
        continue;
      }
      break;
    }
    if (out.empty()) {
      err = "invalid JSON literal";
      return false;
    }
    return true;
  }

  bool parse_number_token(std::string &out, std::string &err)
  {
    out.clear();
    for (;;) {
      const int c = stream_->peek();
      if (fsql_is_digit_char(c) || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E') {
        out.push_back(static_cast<char>(stream_->next()));
        continue;
      }
      break;
    }
    if (out.empty()) {
      err = "invalid JSON number";
      return false;
    }
    return true;
  }

private:
  bool read_hex4(uint32_t &out)
  {
    uint32_t value = 0;
    for (int i = 0; i < 4; ++i) {
      const int c = stream_->next();
      int digit = -1;
      if (c >= '0' && c <= '9') {
        digit = c - '0';
      } else if (c >= 'a' && c <= 'f') {
        digit = c - 'a' + 10;
      } else if (c >= 'A' && c <= 'F') {
        digit = c - 'A' + 10;
      }
      if (digit < 0) {
        return false;
      }
      value = (value << 4) | static_cast<uint32_t>(digit);
    }
    out = value;
    return true;
  }

  FsqlFileStream *stream_;
};

inline std::string fsql_json_line_prefix(const FsqlJsonCursor &cursor)
{
  return "line " + std::to_string(cursor.line()) + ": ";
}

inline bool fsql_json_number_from_token(const std::string &token, FsqlJsonValue &value, std::string &err)
{
  if (!fsql_validate_decimal_syntax(token.data(), token.size())) {
    err = "invalid JSON number \"" + token + "\"";
    return false;
  }
  const bool is_integer = token.find_first_of(".eE") == std::string::npos;
  if (is_integer) {
    int64_t int_value = 0;
    if (fsql_parse_int64_text(token.data(), token.size(), int_value)) {
      value.kind = FSQL_JSON_INT;
      value.int_value = int_value;
      return true;
    }
  }
  double double_value = 0;
  if (!fsql_parse_double_text(token.data(), token.size(), double_value)) {
    err = "JSON number \"" + token + "\" is out of range";
    return false;
  }
  value.kind = FSQL_JSON_DOUBLE;
  value.double_value = double_value;
  return true;
}

inline bool fsql_json_parse_scalar(FsqlJsonCursor &cursor, FsqlJsonValue &value, std::string &err)
{
  const int c = cursor.peek();
  if (c < 0) {
    err = fsql_json_line_prefix(cursor) + "unexpected end of file in JSON";
    return false;
  }
  if (c == '"') {
    value.kind = FSQL_JSON_STRING;
    if (!cursor.parse_string(value.str_value, err)) {
      err = fsql_json_line_prefix(cursor) + err;
      return false;
    }
    return true;
  }
  if (c == 't' || c == 'f' || c == 'n') {
    std::string token;
    if (!cursor.parse_literal_token(token, err)) {
      err = fsql_json_line_prefix(cursor) + err;
      return false;
    }
    if (token == "true") {
      value.kind = FSQL_JSON_BOOL;
      value.bool_value = true;
      return true;
    }
    if (token == "false") {
      value.kind = FSQL_JSON_BOOL;
      value.bool_value = false;
      return true;
    }
    if (token == "null") {
      value.kind = FSQL_JSON_NULL;
      return true;
    }
    err = fsql_json_line_prefix(cursor) + "invalid JSON literal \"" + token + "\"";
    return false;
  }
  if (c == '-' || c == '+' || c == '.' || fsql_is_digit_char(c)) {
    std::string token;
    if (!cursor.parse_number_token(token, err)) {
      err = fsql_json_line_prefix(cursor) + err;
      return false;
    }
    if (!fsql_json_number_from_token(token, value, err)) {
      err = fsql_json_line_prefix(cursor) + err;
      return false;
    }
    return true;
  }
  err = fsql_json_line_prefix(cursor) + "invalid JSON value";
  return false;
}

// Parses one flat JSON object.  Nested objects and arrays are rejected with the
// line number and the offending key because the file SQL layer only supports a
// single level of scalar attributes.
inline bool fsql_json_parse_flat_object(FsqlJsonCursor &cursor,
                                        FsqlJsonObject &object,
                                        std::string &err)
{
  object.clear();
  if (cursor.peek() != '{') {
    err = fsql_json_line_prefix(cursor) + "expected a JSON object";
    return false;
  }
  cursor.get();
  cursor.skip_ws();
  if (cursor.peek() == '}') {
    cursor.get();
    return true;
  }
  for (;;) {
    cursor.skip_ws();
    if (cursor.peek() != '"') {
      err = fsql_json_line_prefix(cursor) + "expected a JSON object key";
      return false;
    }
    std::string key;
    if (!cursor.parse_string(key, err)) {
      err = fsql_json_line_prefix(cursor) + err;
      return false;
    }
    cursor.skip_ws();
    if (cursor.peek() != ':') {
      err = fsql_json_line_prefix(cursor) + "expected ':' after the JSON key \"" + key + "\"";
      return false;
    }
    cursor.get();
    cursor.skip_ws();
    const int value_start = cursor.peek();
    if (value_start == '{' || value_start == '[') {
      err = fsql_json_line_prefix(cursor) + "nested object or array is not supported for key \"" +
            key + "\"";
      return false;
    }
    FsqlJsonValue value;
    if (!fsql_json_parse_scalar(cursor, value, err)) {
      return false;
    }
    bool replaced = false;
    for (size_t i = 0; i < object.size(); ++i) {
      if (object[i].first == key) {
        object[i].second = value;
        replaced = true;
        break;
      }
    }
    if (!replaced) {
      object.push_back(std::make_pair(key, value));
    }
    cursor.skip_ws();
    const int separator = cursor.peek();
    if (separator == ',') {
      cursor.get();
      continue;
    }
    if (separator == '}') {
      cursor.get();
      break;
    }
    err = fsql_json_line_prefix(cursor) + "expected ',' or '}' in a JSON object";
    return false;
  }
  return true;
}

enum FsqlJsonMode : int32_t
{
  FSQL_JSON_MODE_NEWLINE_DELIMITED = 0,
  FSQL_JSON_MODE_ARRAY = 1,
  FSQL_JSON_MODE_OBJECT = 2,
};

class FsqlJsonRowStream
{
public:
  FsqlJsonRowStream()
      : mode_(FSQL_JSON_MODE_NEWLINE_DELIMITED), started_(false), finished_(false), first_element_(true)
  {
    cursor_.init(&stream_);
  }

  int open(const std::string &path, const FileScanOptions &options, std::string &err)
  {
    mode_ = FSQL_JSON_MODE_NEWLINE_DELIMITED;
    started_ = false;
    finished_ = false;
    first_element_ = true;
    const std::string &format = options.json_format;
    const bool auto_detect = format.empty() || fsql_ascii_iequal_literal(format, "auto") ||
                             fsql_ascii_iequal_literal(format, "json");
    if (!auto_detect) {
      if (fsql_ascii_iequal_literal(format, "newline_delimited") ||
          fsql_ascii_iequal_literal(format, "ndjson") ||
          fsql_ascii_iequal_literal(format, "jsonl")) {
        mode_ = FSQL_JSON_MODE_NEWLINE_DELIMITED;
      } else if (fsql_ascii_iequal_literal(format, "array")) {
        mode_ = FSQL_JSON_MODE_ARRAY;
      } else if (fsql_ascii_iequal_literal(format, "object")) {
        mode_ = FSQL_JSON_MODE_OBJECT;
      } else {
        err = "unsupported json format: " + format;
        return -1;
      }
    }
    if (stream_.open(path) != 0) {
      err = "cannot open file: " + path;
      return -1;
    }
    if (auto_detect) {
      // "auto": a leading '[' means a top level array, anything else is read as
      // newline delimited objects.
      cursor_.skip_ws();
      mode_ = cursor_.peek() == '[' ? FSQL_JSON_MODE_ARRAY : FSQL_JSON_MODE_NEWLINE_DELIMITED;
    }
    return 0;
  }

  // Returns 1 for a row, 0 at the end of the stream and -1 on error.
  int next_row(FsqlJsonObject &object, std::string &err)
  {
    object.clear();
    if (finished_) {
      return 0;
    }
    if (mode_ == FSQL_JSON_MODE_ARRAY) {
      return next_row_from_array(object, err);
    }
    if (mode_ == FSQL_JSON_MODE_OBJECT) {
      return next_row_from_single_object(object, err);
    }
    return next_row_newline_delimited(object, err);
  }

private:
  int next_row_newline_delimited(FsqlJsonObject &object, std::string &err)
  {
    cursor_.skip_ws();
    int c = cursor_.peek();
    if (c < 0) {
      finished_ = true;
      return 0;
    }
    if (c != '{') {
      err = fsql_json_line_prefix(cursor_) + "expected a JSON object";
      return -1;
    }
    if (!fsql_json_parse_flat_object(cursor_, object, err)) {
      return -1;
    }
    for (;;) {
      c = cursor_.peek();
      if (c == ' ' || c == '\t' || c == '\r') {
        cursor_.get();
        continue;
      }
      break;
    }
    if (c < 0) {
      finished_ = true;
      return 1;
    }
    if (c == '\n') {
      cursor_.get();
      return 1;
    }
    err = fsql_json_line_prefix(cursor_) + "unexpected data after the JSON object";
    return -1;
  }

  int next_row_from_array(FsqlJsonObject &object, std::string &err)
  {
    if (!started_) {
      cursor_.skip_ws();
      if (cursor_.peek() != '[') {
        err = fsql_json_line_prefix(cursor_) + "expected a JSON array";
        return -1;
      }
      cursor_.get();
      started_ = true;
    }
    cursor_.skip_ws();
    int c = cursor_.peek();
    if (c < 0) {
      err = fsql_json_line_prefix(cursor_) + "unexpected end of file inside the JSON array";
      return -1;
    }
    if (c == ']') {
      cursor_.get();
      finished_ = true;
      cursor_.skip_ws();
      if (cursor_.peek() >= 0) {
        err = fsql_json_line_prefix(cursor_) + "unexpected data after the JSON array";
        return -1;
      }
      return 0;
    }
    if (!first_element_) {
      if (c != ',') {
        err = fsql_json_line_prefix(cursor_) + "expected ',' or ']' in the JSON array";
        return -1;
      }
      cursor_.get();
      cursor_.skip_ws();
      c = cursor_.peek();
      if (c == ']') {
        cursor_.get();
        finished_ = true;
        cursor_.skip_ws();
        if (cursor_.peek() >= 0) {
          err = fsql_json_line_prefix(cursor_) + "unexpected data after the JSON array";
          return -1;
        }
        return 0;
      }
      if (c < 0) {
        err = fsql_json_line_prefix(cursor_) + "unexpected end of file inside the JSON array";
        return -1;
      }
    }
    if (c != '{') {
      err = fsql_json_line_prefix(cursor_) + "expected a JSON object in the JSON array";
      return -1;
    }
    if (!fsql_json_parse_flat_object(cursor_, object, err)) {
      return -1;
    }
    first_element_ = false;
    return 1;
  }

  int next_row_from_single_object(FsqlJsonObject &object, std::string &err)
  {
    cursor_.skip_ws();
    const int c = cursor_.peek();
    if (c < 0) {
      err = "empty file: no JSON object found";
      return -1;
    }
    if (c != '{') {
      err = fsql_json_line_prefix(cursor_) + "expected a single JSON object";
      return -1;
    }
    if (!fsql_json_parse_flat_object(cursor_, object, err)) {
      return -1;
    }
    cursor_.skip_ws();
    if (cursor_.peek() >= 0) {
      err = fsql_json_line_prefix(cursor_) + "unexpected data after the JSON object";
      return -1;
    }
    finished_ = true;
    return 1;
  }

  FsqlFileStream stream_;
  FsqlJsonCursor cursor_;
  int32_t mode_;
  bool started_;
  bool finished_;
  bool first_element_;
};

// ---------------------------------------------------------------------------
// Projection helper shared by the readers
// ---------------------------------------------------------------------------

int fsql_apply_projection(const std::vector<FileColumn> &all,
                          const std::vector<int32_t> &projection,
                          std::vector<FileColumn> &columns,
                          std::vector<int32_t> &source_indexes,
                          std::string &err)
{
  columns.clear();
  source_indexes.clear();
  if (projection.empty()) {
    for (size_t i = 0; i < all.size(); ++i) {
      source_indexes.push_back(static_cast<int32_t>(i));
      columns.push_back(all[i]);
    }
    return 0;
  }
  for (size_t i = 0; i < projection.size(); ++i) {
    const int32_t index = projection[i];
    if (index < 0 || static_cast<size_t>(index) >= all.size()) {
      err = "projection column index " + std::to_string(index) + " is out of range";
      columns.clear();
      source_indexes.clear();
      return -1;
    }
    source_indexes.push_back(index);
    columns.push_back(all[static_cast<size_t>(index)]);
  }
  return 0;
}

// ---------------------------------------------------------------------------
// CSV batch reader
// ---------------------------------------------------------------------------

class FsqlCsvReader : public FileBatchReader
{
public:
  FsqlCsvReader() : eof_(false), opened_(false) {}
  ~FsqlCsvReader() override {}

  int open(const std::string &path, const FileScanOptions &options, std::string &err) override
  {
    err.clear();
    opened_ = false;
    eof_ = false;
    options_ = options;
    identity_ = FileIdentity();
    std::vector<FileColumn> all_columns;
    if (infer_csv_schema(path, options, all_columns, identity_, err) != 0) {
      err = std::string("read_csv: ") + err;
      return FAILED;
    }
    if (fsql_apply_projection(all_columns, options.projection, columns_, source_indexes_, err) != 0) {
      err = std::string("read_csv: ") + err;
      return FAILED;
    }
    if (stream_.open(path) != 0) {
      err = "cannot open file for reading: " + path;
      return FAILED;
    }
    parser_.open(stream_, options_);
    if (options_.header) {
      std::vector<FsqlCsvField> header;
      const int ret = parser_.read_record(header, err);
      if (ret < 0) {
        err = "read_csv: " + err;
        return FAILED;
      }
      if (ret == 0) {
        err = "read_csv: empty file: " + path;
        return FAILED;
      }
    }
    opened_ = true;
    return SUCCESS;
  }

  const std::vector<FileColumn> &columns() const override { return columns_; }

  int read_batch(int64_t max_rows, std::vector<std::vector<Cell>> &rows, std::string &err) override
  {
    err.clear();
    if (!opened_) {
      err = "read_csv: reader is not open";
      return FAILED;
    }
    if (eof_) {
      return EOF_REACHED;
    }
    if (max_rows <= 0) {
      return SUCCESS;
    }
    std::vector<FsqlCsvField> record;
    std::vector<Cell> row;
    int64_t produced = 0;
    while (produced < max_rows) {
      const int ret = parser_.read_record(record, err);
      if (ret < 0) {
        err = "read_csv: " + err;
        return FAILED;
      }
      if (ret == 0) {
        eof_ = true;
        break;
      }
      row.clear();
      row.resize(columns_.size());
      for (size_t i = 0; i < columns_.size(); ++i) {
        const int32_t source = source_indexes_[i];
        if (static_cast<size_t>(source) < record.size()) {
          fsql_csv_field_to_cell(
              record[static_cast<size_t>(source)], options_.null_str, columns_[i].type, row[i]);
        } else {
          fsql_reset_cell(row[i], columns_[i].type);
        }
      }
      rows.push_back(std::move(row));
      ++produced;
    }
    if (produced == 0) {
      return eof_ ? EOF_REACHED : SUCCESS;
    }
    return SUCCESS;
  }

  FileIdentity identity() const override { return identity_; }

private:
  FileScanOptions options_;
  FsqlFileStream stream_;
  FsqlCsvParser parser_;
  std::vector<FileColumn> columns_;
  std::vector<int32_t> source_indexes_;
  FileIdentity identity_;
  bool eof_;
  bool opened_;
};

// ---------------------------------------------------------------------------
// JSON batch reader
// ---------------------------------------------------------------------------

class FsqlJsonReader : public FileBatchReader
{
public:
  FsqlJsonReader() : eof_(false), opened_(false) {}
  ~FsqlJsonReader() override {}

  int open(const std::string &path, const FileScanOptions &options, std::string &err) override
  {
    err.clear();
    opened_ = false;
    eof_ = false;
    options_ = options;
    identity_ = FileIdentity();
    std::vector<FileColumn> all_columns;
    if (infer_json_schema(path, options, all_columns, identity_, err) != 0) {
      err = std::string("read_json: ") + err;
      return FAILED;
    }
    all_columns_ = all_columns;
    if (fsql_apply_projection(all_columns, options.projection, columns_, source_indexes_, err) != 0) {
      err = std::string("read_json: ") + err;
      return FAILED;
    }
    if (rows_.open(path, options_, err) != 0) {
      err = std::string("read_json: ") + err;
      return FAILED;
    }
    opened_ = true;
    return SUCCESS;
  }

  const std::vector<FileColumn> &columns() const override { return columns_; }

  int read_batch(int64_t max_rows, std::vector<std::vector<Cell>> &rows, std::string &err) override
  {
    err.clear();
    if (!opened_) {
      err = "read_json: reader is not open";
      return FAILED;
    }
    if (eof_) {
      return EOF_REACHED;
    }
    if (max_rows <= 0) {
      return SUCCESS;
    }
    std::vector<Cell> row;
    int64_t produced = 0;
    while (produced < max_rows) {
      const int ret = rows_.next_row(object_, err);
      if (ret < 0) {
        err = "read_json: " + err;
        return FAILED;
      }
      if (ret == 0) {
        eof_ = true;
        break;
      }
      row.clear();
      row.resize(columns_.size());
      for (size_t i = 0; i < columns_.size(); ++i) {
        const int32_t source = source_indexes_[i];
        const FsqlJsonValue *value = nullptr;
        if (static_cast<size_t>(source) < all_columns_.size()) {
          value = fsql_json_find_field(object_, all_columns_[static_cast<size_t>(source)].name);
        }
        if (value == nullptr) {
          fsql_reset_cell(row[i], columns_[i].type);
        } else {
          fsql_json_value_to_cell(*value, columns_[i].type, row[i]);
        }
      }
      rows.push_back(std::move(row));
      ++produced;
    }
    if (produced == 0) {
      return eof_ ? EOF_REACHED : SUCCESS;
    }
    return SUCCESS;
  }

  FileIdentity identity() const override { return identity_; }

private:
  FileScanOptions options_;
  FsqlJsonRowStream rows_;
  FsqlJsonObject object_;
  std::vector<FileColumn> all_columns_;
  std::vector<FileColumn> columns_;
  std::vector<int32_t> source_indexes_;
  FileIdentity identity_;
  bool eof_;
  bool opened_;
};

// ---------------------------------------------------------------------------
// CSV writer helpers
// ---------------------------------------------------------------------------

inline void fsql_cell_to_text(const Cell &cell, std::string &text)
{
  text.clear();
  switch (cell.type) {
    case FILE_COL_BIGINT:
      text = std::to_string(cell.int_value);
      break;
    case FILE_COL_DOUBLE:
      text = fsql_double_to_text(cell.double_value);
      break;
    case FILE_COL_BOOLEAN:
      text = cell.bool_value ? "1" : "0";
      break;
    case FILE_COL_DATE:
    case FILE_COL_DATETIME:
    case FILE_COL_VARCHAR:
    case FILE_COL_NULL:
    default:
      text = cell.str_value;
      break;
  }
}

inline bool fsql_csv_text_needs_quote(const std::string &text, const CsvWriteOptions &options)
{
  // An empty text value must be quoted: an unquoted empty field reads back as NULL.
  if (text.empty()) {
    return true;
  }
  if (!options.null_str.empty() && text == options.null_str) {
    return true;
  }
  for (size_t i = 0; i < text.size(); ++i) {
    const char c = text[i];
    if (c == options.delimiter || c == options.quote || c == '\r' || c == '\n') {
      return true;
    }
    if (options.escape != options.quote && c == options.escape) {
      return true;
    }
  }
  return false;
}

inline void fsql_csv_append_quoted(const std::string &text,
                                   const CsvWriteOptions &options,
                                   std::string &out)
{
  out.push_back(options.quote);
  for (size_t i = 0; i < text.size(); ++i) {
    const char c = text[i];
    if (c == options.quote) {
      if (options.escape == options.quote) {
        out.push_back(options.quote);
        out.push_back(options.quote);
      } else {
        out.push_back(options.escape);
        out.push_back(options.quote);
      }
    } else if (options.escape != options.quote && c == options.escape) {
      out.push_back(options.escape);
      out.push_back(options.escape);
    } else {
      out.push_back(c);
    }
  }
  out.push_back(options.quote);
}

class FsqlCsvWriter : public FileBatchWriter
{
public:
  explicit FsqlCsvWriter(const CsvWriteOptions &options)
      : options_(options), fp_(nullptr), opened_(false)
  {
  }

  ~FsqlCsvWriter() override { abort(); }

  int open(const std::string &path, std::string &err) override
  {
    err.clear();
    if (opened_) {
      abort();
    }
    if (path.empty()) {
      err = "empty output file path";
      return -1;
    }
    std::error_code ec;
    const bool exists = std::filesystem::exists(std::filesystem::path(path), ec);
    if (ec) {
      err = "cannot check whether the output file exists: " + path + ": " + ec.message();
      return -1;
    }
    if (exists) {
      err = "file exists: " + path;
      return -1;
    }
    final_path_ = path;
    temp_path_ = path + ".tmp";
    fp_ = std::fopen(temp_path_.c_str(), "wb");
    if (fp_ == nullptr) {
      temp_path_.clear();
      final_path_.clear();
      err = "cannot create temporary output file: " + path + ".tmp";
      return -1;
    }
    opened_ = true;
    return 0;
  }

  int write_row(const std::vector<Cell> &row, std::string &err) override
  {
    err.clear();
    if (!opened_ || fp_ == nullptr) {
      err = "csv writer is not open";
      return -1;
    }
    std::string buffer;
    std::string text;
    for (size_t i = 0; i < row.size(); ++i) {
      if (i > 0) {
        buffer.push_back(options_.delimiter);
      }
      const Cell &cell = row[i];
      if (cell.is_null) {
        buffer.append(options_.null_str);
        continue;
      }
      fsql_cell_to_text(cell, text);
      if (fsql_csv_text_needs_quote(text, options_)) {
        fsql_csv_append_quoted(text, options_, buffer);
      } else {
        buffer.append(text);
      }
    }
    buffer.append(options_.line_terminator);
    if (!buffer.empty() && std::fwrite(buffer.data(), 1, buffer.size(), fp_) != buffer.size()) {
      err = "failed to write a csv row to " + temp_path_;
      return -1;
    }
    return 0;
  }

  int close(std::string &err) override
  {
    err.clear();
    if (!opened_) {
      err = "csv writer is not open";
      return -1;
    }
    bool flushed = true;
    if (fp_ != nullptr) {
      if (std::fflush(fp_) != 0) {
        flushed = false;
      }
      if (std::fclose(fp_) != 0) {
        flushed = false;
      }
      fp_ = nullptr;
    }
    opened_ = false;
    if (!flushed) {
      err = "failed to flush the temporary output file: " + temp_path_;
      remove_temp_file();
      final_path_.clear();
      return -1;
    }
    std::error_code ec;
    std::filesystem::rename(
        std::filesystem::path(temp_path_), std::filesystem::path(final_path_), ec);
    if (ec) {
      err = "failed to rename " + temp_path_ + " to " + final_path_ + ": " + ec.message();
      remove_temp_file();
      final_path_.clear();
      return -1;
    }
    temp_path_.clear();
    final_path_.clear();
    return 0;
  }

  void abort() override
  {
    if (fp_ != nullptr) {
      std::fclose(fp_);
      fp_ = nullptr;
    }
    opened_ = false;
    remove_temp_file();
    final_path_.clear();
  }

private:
  void remove_temp_file()
  {
    if (temp_path_.empty()) {
      return;
    }
    std::error_code ec;
    std::filesystem::remove(std::filesystem::path(temp_path_), ec);
    temp_path_.clear();
  }

  CsvWriteOptions options_;
  std::FILE *fp_;
  std::string temp_path_;
  std::string final_path_;
  bool opened_;
};

} // namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

const char *file_column_type_name(FileColumnType type)
{
  switch (type) {
    case FILE_COL_BIGINT:
      return "BIGINT";
    case FILE_COL_DOUBLE:
      return "DOUBLE";
    case FILE_COL_BOOLEAN:
      return "BOOLEAN";
    case FILE_COL_VARCHAR:
      return "VARCHAR";
    case FILE_COL_DATE:
      return "DATE";
    case FILE_COL_DATETIME:
      return "DATETIME";
    case FILE_COL_NULL:
      return "NULL";
    default:
      return "NULL";
  }
}

bool detect_format_by_extension(const std::string &path, int32_t &format)
{
  const size_t dot = path.find_last_of('.');
  const size_t separator = path.find_last_of("/\\");
  if (dot == std::string::npos || (separator != std::string::npos && dot < separator)) {
    return false;
  }
  const std::string extension = path.substr(dot);
  if (fsql_ascii_iequal_literal(extension, ".csv")) {
    format = FILE_FORMAT_CSV;
    return true;
  }
  if (fsql_ascii_iequal_literal(extension, ".json") || fsql_ascii_iequal_literal(extension, ".jsonl") ||
      fsql_ascii_iequal_literal(extension, ".ndjson")) {
    format = FILE_FORMAT_JSON;
    return true;
  }
  if (fsql_ascii_iequal_literal(extension, ".parquet") ||
      fsql_ascii_iequal_literal(extension, ".parq") || fsql_ascii_iequal_literal(extension, ".pq")) {
    format = FILE_FORMAT_PARQUET;
    return true;
  }
  return false;
}

int read_file_identity(const std::string &path, FileIdentity &identity)
{
  identity = FileIdentity();
  if (path.empty()) {
    return -1;
  }
  std::error_code ec;
  const std::filesystem::path fs_path(path);
  const uintmax_t size = std::filesystem::file_size(fs_path, ec);
  if (ec) {
    return -1;
  }
  const std::filesystem::file_time_type write_time = std::filesystem::last_write_time(fs_path, ec);
  if (ec) {
    return -1;
  }
  const std::chrono::nanoseconds since_epoch =
      std::chrono::duration_cast<std::chrono::nanoseconds>(write_time.time_since_epoch());
  if (size > static_cast<uintmax_t>(9223372036854775807LL)) {
    return -1;
  }
  identity.size = static_cast<int64_t>(size);
  identity.mtime_ns = static_cast<int64_t>(since_epoch.count());
  identity.valid = true;
  return 0;
}

int infer_csv_schema(const std::string &path,
                     const FileScanOptions &options,
                     std::vector<FileColumn> &columns,
                     FileIdentity &identity,
                     std::string &err)
{
  err.clear();
  columns.clear();
  identity = FileIdentity();
  if (read_file_identity(path, identity) != 0) {
    identity = FileIdentity();
    err = "cannot read the file identity: " + path;
    return -1;
  }
  FsqlFileStream stream;
  if (stream.open(path) != 0) {
    err = "cannot open file: " + path;
    return -1;
  }
  FsqlCsvParser parser;
  parser.open(stream, options);

  std::vector<FsqlCsvField> record;
  int ret = parser.read_record(record, err);
  if (ret < 0) {
    err = path + ": " + err;
    return -1;
  }
  if (ret == 0) {
    err = "empty file: no rows found in " + path;
    return -1;
  }

  const size_t column_count = record.size();
  if (column_count == 0) {
    err = "empty file: no columns found in " + path;
    return -1;
  }
  std::vector<std::string> names;
  names.reserve(column_count);
  if (options.header) {
    for (size_t i = 0; i < column_count; ++i) {
      names.push_back(fsql_make_unique_column_name(names, record[i].text, i));
    }
  } else {
    for (size_t i = 0; i < column_count; ++i) {
      names.push_back(fsql_default_column_name(i));
    }
  }

  std::vector<int32_t> masks(column_count, 0);
  std::vector<char> null_seen(column_count, 0);
  int64_t data_rows = 0;
  bool pending_first_record = !options.header;
  for (;;) {
    if (!pending_first_record) {
      ret = parser.read_record(record, err);
      if (ret < 0) {
        err = path + ": " + err;
        return -1;
      }
      if (ret == 0) {
        break;
      }
    }
    ++data_rows;
    for (size_t i = 0; i < column_count; ++i) {
      if (i >= record.size()) {
        null_seen[i] = 1;
        continue;
      }
      const FsqlCsvField &field = record[i];
      if (fsql_csv_field_is_null(field, options.null_str)) {
        null_seen[i] = 1;
        continue;
      }
      masks[i] |= fsql_classify_text_value(field.text.data(), field.text.size());
    }
    pending_first_record = false;
  }
  if (data_rows == 0) {
    err = "empty file: no data rows found in " + path;
    return -1;
  }

  columns.resize(column_count);
  for (size_t i = 0; i < column_count; ++i) {
    columns[i].name = names[i];
    columns[i].type = fsql_mask_to_column_type(masks[i]);
    columns[i].nullable = null_seen[i] != 0;
  }
  return 0;
}

int infer_json_schema(const std::string &path,
                      const FileScanOptions &options,
                      std::vector<FileColumn> &columns,
                      FileIdentity &identity,
                      std::string &err)
{
  err.clear();
  columns.clear();
  identity = FileIdentity();
  if (read_file_identity(path, identity) != 0) {
    identity = FileIdentity();
    err = "cannot read the file identity: " + path;
    return -1;
  }
  FsqlJsonRowStream rows;
  if (rows.open(path, options, err) != 0) {
    return -1;
  }
  std::vector<int32_t> masks;
  std::vector<char> null_seen;
  std::vector<int64_t> appearances;
  std::unordered_map<std::string, size_t> key_index;
  FsqlJsonObject object;
  int64_t row_count = 0;
  for (;;) {
    const int ret = rows.next_row(object, err);
    if (ret < 0) {
      return -1;
    }
    if (ret == 0) {
      break;
    }
    ++row_count;
    for (size_t i = 0; i < object.size(); ++i) {
      const std::string &key = object[i].first;
      const FsqlJsonValue &value = object[i].second;
      std::unordered_map<std::string, size_t>::iterator found = key_index.find(key);
      size_t column;
      if (found == key_index.end()) {
        column = columns.size();
        key_index.insert(std::make_pair(key, column));
        FileColumn column_desc;
        column_desc.name = key;
        column_desc.type = FILE_COL_NULL;
        column_desc.nullable = false;
        columns.push_back(column_desc);
        masks.push_back(0);
        null_seen.push_back(0);
        appearances.push_back(0);
      } else {
        column = found->second;
      }
      ++appearances[column];
      if (value.kind == FSQL_JSON_NULL) {
        null_seen[column] = 1;
      } else {
        masks[column] |= fsql_json_value_bits(value);
      }
    }
  }
  if (row_count == 0) {
    err = "empty file: no rows found in " + path;
    return -1;
  }
  if (columns.empty()) {
    err = "empty file: no columns found in " + path;
    return -1;
  }
  for (size_t i = 0; i < columns.size(); ++i) {
    columns[i].type = fsql_json_mask_to_column_type(masks[i]);
    columns[i].nullable = null_seen[i] != 0 || appearances[i] < row_count;
  }
  return 0;
}

FileBatchReader *create_file_reader(int32_t format)
{
  switch (format) {
    case FILE_FORMAT_CSV:
      return new (std::nothrow) FsqlCsvReader();
    case FILE_FORMAT_JSON:
      return new (std::nothrow) FsqlJsonReader();
    case FILE_FORMAT_PARQUET:
      return create_parquet_reader();
    default:
      return nullptr;
  }
}

FileBatchWriter *create_csv_writer(const CsvWriteOptions &options)
{
  return new (std::nothrow) FsqlCsvWriter(options);
}

} // namespace file_sql
} // namespace sql
} // namespace oceanbase
