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

#include "ob_parquet_format.h"

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <algorithm>
#include <fstream>
#include <limits>
#include <new>
#include <string>
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
// Hard limits.  A malformed or hostile file must never be able to make us
// allocate an unbounded amount of memory.
// ---------------------------------------------------------------------------
const int64_t PQ_MAX_ALLOC_BYTES = 256LL * 1024 * 1024;      // one page / chunk buffer
const int64_t PQ_MAX_LIST_ELEMENTS = 64LL * 1024 * 1024;     // metadata list length
const int64_t PQ_MAX_DICT_VALUES = 2LL * 1024 * 1024;        // decoded dictionary entries
const int64_t PQ_MAX_PAGE_VALUES = 2LL * 1024 * 1024;        // values in one data page
const int64_t PQ_MAX_BATCH_ROWS = 65536;                     // internal decode granularity
const int64_t PQ_MAX_PAGE_HEADER_BYTES = 1024LL * 1024;      // a PageHeader is tiny

// ---------------------------------------------------------------------------
// Parquet format constants (see parquet-format/src/main/thrift/parquet.thrift).
// ---------------------------------------------------------------------------
enum PqPhysicalType
{
  PQ_PHYS_BOOLEAN = 0,
  PQ_PHYS_INT32 = 1,
  PQ_PHYS_INT64 = 2,
  PQ_PHYS_INT96 = 3,
  PQ_PHYS_FLOAT = 4,
  PQ_PHYS_DOUBLE = 5,
  PQ_PHYS_BYTE_ARRAY = 6,
  PQ_PHYS_FIXED_LEN_BYTE_ARRAY = 7,
};

enum PqConvertedType
{
  PQ_CT_UTF8 = 0,
  PQ_CT_MAP = 1,
  PQ_CT_MAP_KEY_VALUE = 2,
  PQ_CT_LIST = 3,
  PQ_CT_ENUM = 4,
  PQ_CT_DECIMAL = 5,
  PQ_CT_DATE = 6,
  PQ_CT_TIME_MILLIS = 7,
  PQ_CT_TIME_MICROS = 8,
  PQ_CT_TIMESTAMP_MILLIS = 9,
  PQ_CT_TIMESTAMP_MICROS = 10,
  PQ_CT_UINT_8 = 11,
  PQ_CT_UINT_16 = 12,
  PQ_CT_UINT_32 = 13,
  PQ_CT_UINT_64 = 14,
  PQ_CT_INT_8 = 15,
  PQ_CT_INT_16 = 16,
  PQ_CT_INT_32 = 17,
  PQ_CT_INT_64 = 18,
  PQ_CT_JSON = 19,
  PQ_CT_BSON = 20,
  PQ_CT_INTERVAL = 21,
};

enum PqRepetitionType
{
  PQ_REP_REQUIRED = 0,
  PQ_REP_OPTIONAL = 1,
  PQ_REP_REPEATED = 2,
};

enum PqEncoding
{
  PQ_ENC_PLAIN = 0,
  PQ_ENC_PLAIN_DICTIONARY = 2,
  PQ_ENC_RLE = 3,
  PQ_ENC_BIT_PACKED = 4,
  PQ_ENC_RLE_DICTIONARY = 8,
};

enum PqCodec
{
  PQ_CODEC_UNCOMPRESSED = 0,
  PQ_CODEC_SNAPPY = 1,
  PQ_CODEC_GZIP = 2,
  PQ_CODEC_LZO = 3,
  PQ_CODEC_BROTLI = 4,
  PQ_CODEC_LZ4 = 5,
  PQ_CODEC_ZSTD = 6,
  PQ_CODEC_LZ4_RAW = 7,
};

enum PqPageType
{
  PQ_PAGE_DATA = 0,
  PQ_PAGE_INDEX = 1,
  PQ_PAGE_DICTIONARY = 2,
  PQ_PAGE_DATA_V2 = 3,
};

// Thrift compact protocol type ids.
enum PqThriftType
{
  PQ_TT_STOP = 0,
  PQ_TT_TRUE = 1,
  PQ_TT_FALSE = 2,
  PQ_TT_BYTE = 3,
  PQ_TT_I16 = 4,
  PQ_TT_I32 = 5,
  PQ_TT_I64 = 6,
  PQ_TT_DOUBLE = 7,
  PQ_TT_BINARY = 8,
  PQ_TT_LIST = 9,
  PQ_TT_SET = 10,
  PQ_TT_MAP = 11,
  PQ_TT_STRUCT = 12,
};

// ---------------------------------------------------------------------------
// Small helpers.
// ---------------------------------------------------------------------------
bool pq_checked_add(int64_t a, int64_t b, int64_t &out)
{
  if ((b > 0 && a > std::numeric_limits<int64_t>::max() - b) ||
      (b < 0 && a < std::numeric_limits<int64_t>::min() - b)) {
    return false;
  }
  out = a + b;
  return true;
}

bool pq_checked_mul(int64_t a, int64_t b, int64_t &out)
{
  if (a == 0 || b == 0) {
    out = 0;
    return true;
  }
  if (a > 0 && b > 0 && a > std::numeric_limits<int64_t>::max() / b) {
    return false;
  }
  if (a < 0 || b < 0) {
    return false;
  }
  out = a * b;
  return true;
}

// Bit width needed to represent [0, max_value].
int32_t pq_bit_width(int64_t max_value)
{
  int32_t width = 0;
  while (max_value > 0) {
    ++width;
    max_value >>= 1;
  }
  return width;
}

// ---------------------------------------------------------------------------
// Thrift compact protocol reader.
// ---------------------------------------------------------------------------
class PqThriftReader
{
public:
  PqThriftReader(const uint8_t *data, int64_t len)
      : data_(data), p_(data), end_(data + len) {}

  bool ok() const { return ok_; }
  int64_t consumed() const { return static_cast<int64_t>(p_ - data_); }
  int64_t remaining() const { return static_cast<int64_t>(end_ - p_); }
  bool eof() const { return p_ >= end_; }

  bool read_u8(uint8_t &value)
  {
    if (p_ >= end_) {
      ok_ = false;
      return false;
    }
    value = *p_++;
    return true;
  }

  bool read_raw(int64_t len, const uint8_t *&data)
  {
    if (len < 0 || len > static_cast<int64_t>(end_ - p_)) {
      ok_ = false;
      return false;
    }
    data = p_;
    p_ += len;
    return true;
  }

  bool read_uvarint(uint64_t &value)
  {
    value = 0;
    int32_t shift = 0;
    while (shift <= 63) {
      uint8_t byte = 0;
      if (!read_u8(byte)) {
        return false;
      }
      value |= static_cast<uint64_t>(byte & 0x7f) << shift;
      if ((byte & 0x80) == 0) {
        return true;
      }
      shift += 7;
    }
    ok_ = false;
    return false;
  }

  bool read_zigzag(int64_t &value)
  {
    uint64_t raw = 0;
    if (!read_uvarint(raw)) {
      return false;
    }
    const int64_t magnitude = static_cast<int64_t>(raw >> 1);
    value = (raw & 1) != 0 ? ~magnitude : magnitude;
    return true;
  }

  bool read_binary(const uint8_t *&data, int64_t &len)
  {
    uint64_t size = 0;
    if (!read_uvarint(size)) {
      return false;
    }
    if (size > static_cast<uint64_t>(PQ_MAX_ALLOC_BYTES) ||
        size > static_cast<uint64_t>(remaining())) {
      ok_ = false;
      return false;
    }
    len = static_cast<int64_t>(size);
    return read_raw(len, data);
  }

  bool read_list_header(uint8_t &elem_type, int64_t &count)
  {
    uint8_t header = 0;
    if (!read_u8(header)) {
      return false;
    }
    elem_type = static_cast<uint8_t>(header & 0x0f);
    int64_t size = static_cast<int64_t>((header >> 4) & 0x0f);
    if (size == 15) {
      uint64_t long_size = 0;
      if (!read_uvarint(long_size)) {
        return false;
      }
      if (long_size > static_cast<uint64_t>(PQ_MAX_LIST_ELEMENTS)) {
        ok_ = false;
        return false;
      }
      size = static_cast<int64_t>(long_size);
    }
    count = size;
    return true;
  }

  bool skip_list_element(uint8_t elem_type, int32_t depth)
  {
    if (elem_type == PQ_TT_TRUE || elem_type == PQ_TT_FALSE) {
      uint8_t byte = 0;
      return read_u8(byte);
    }
    return skip_value(elem_type, depth);
  }

  bool skip_struct(int32_t depth)
  {
    for (;;) {
      uint8_t header = 0;
      if (!read_u8(header)) {
        return false;
      }
      if (header == 0) {
        return true;
      }
      const uint8_t type = static_cast<uint8_t>(header & 0x0f);
      const int32_t delta = static_cast<int32_t>((header >> 4) & 0x0f);
      if (delta == 0) {
        int64_t explicit_id = 0;
        if (!read_zigzag(explicit_id)) {
          return false;
        }
      }
      if (!skip_value(type, depth)) {
        return false;
      }
    }
  }

  bool skip_value(uint8_t type, int32_t depth)
  {
    if (depth > 64) {
      ok_ = false;
      return false;
    }
    switch (type) {
      case PQ_TT_TRUE:
      case PQ_TT_FALSE:
        return true;
      case PQ_TT_BYTE: {
        uint8_t byte = 0;
        return read_u8(byte);
      }
      case PQ_TT_I16:
      case PQ_TT_I32:
      case PQ_TT_I64: {
        uint64_t value = 0;
        return read_uvarint(value);
      }
      case PQ_TT_DOUBLE: {
        const uint8_t *ignored = nullptr;
        return read_raw(8, ignored);
      }
      case PQ_TT_BINARY: {
        const uint8_t *ignored = nullptr;
        int64_t len = 0;
        return read_binary(ignored, len);
      }
      case PQ_TT_LIST:
      case PQ_TT_SET: {
        uint8_t elem_type = 0;
        int64_t count = 0;
        if (!read_list_header(elem_type, count)) {
          return false;
        }
        for (int64_t i = 0; i < count; ++i) {
          if (!skip_list_element(elem_type, depth + 1)) {
            return false;
          }
        }
        return true;
      }
      case PQ_TT_MAP: {
        uint64_t count = 0;
        if (!read_uvarint(count)) {
          return false;
        }
        if (count > 0) {
          uint8_t types = 0;
          if (!read_u8(types)) {
            return false;
          }
          const uint8_t key_type = static_cast<uint8_t>((types >> 4) & 0x0f);
          const uint8_t value_type = static_cast<uint8_t>(types & 0x0f);
          for (uint64_t i = 0; i < count; ++i) {
            if (!skip_value(key_type, depth + 1) || !skip_value(value_type, depth + 1)) {
              return false;
            }
          }
        }
        return true;
      }
      case PQ_TT_STRUCT:
        return skip_struct(depth + 1);
      default:
        ok_ = false;
        return false;
    }
  }

private:
  const uint8_t *data_;
  const uint8_t *p_;
  const uint8_t *end_;
  bool ok_ = true;
};

// Iterates the fields of one thrift compact struct.
class PqStructFieldReader
{
public:
  explicit PqStructFieldReader(PqThriftReader &reader) : reader_(reader) {}

  // 1: field present, 0: end of struct, -1: malformed input.
  int next_field(int32_t &field_id, uint8_t &type)
  {
    uint8_t header = 0;
    if (!reader_.read_u8(header)) {
      return -1;
    }
    if (header == 0) {
      return 0;
    }
    const uint8_t compact_type = static_cast<uint8_t>(header & 0x0f);
    const int32_t delta = static_cast<int32_t>((header >> 4) & 0x0f);
    if (delta == 0) {
      int64_t explicit_id = 0;
      if (!reader_.read_zigzag(explicit_id)) {
        return -1;
      }
      field_id = static_cast<int32_t>(explicit_id);
    } else {
      field_id += delta;
    }
    type = compact_type;
    return 1;
  }

private:
  PqThriftReader &reader_;
};

// ---------------------------------------------------------------------------
// Raw Snappy decompression.  Only the uncompressed-length preamble plus the
// four tag kinds are needed; copies are expanded byte by byte so overlapping
// copies (run length compression) behave correctly.
// ---------------------------------------------------------------------------
int pq_snappy_uncompress(const uint8_t *in,
                         int64_t in_len,
                         int64_t expected_size,
                         std::vector<uint8_t> &out,
                         std::string &err)
{
  out.clear();
  if (in_len == 0) {
    // Data page v2 chunks that contain only nulls carry no value bytes at all,
    // not even a snappy length preamble.
    if (expected_size == 0 || expected_size < 0) {
      return 0;
    }
    err = "parquet: snappy block is truncated";
    return -1;
  }
  int64_t pos = 0;
  uint64_t declared = 0;
  int32_t shift = 0;
  bool have_len = false;
  while (pos < in_len && shift <= 63) {
    const uint8_t byte = in[pos++];
    declared |= static_cast<uint64_t>(byte & 0x7f) << shift;
    if ((byte & 0x80) == 0) {
      have_len = true;
      break;
    }
    shift += 7;
  }
  if (!have_len) {
    err = "parquet: invalid snappy stream (missing length preamble)";
    return -1;
  }
  if (declared > static_cast<uint64_t>(PQ_MAX_ALLOC_BYTES)) {
    err = "parquet: snappy block is too large";
    return -1;
  }
  int64_t out_size = static_cast<int64_t>(declared);
  if (expected_size > 0) {
    if (declared != static_cast<uint64_t>(expected_size)) {
      err = "parquet: snappy length mismatch";
      return -1;
    }
    out_size = expected_size;
  }
  out.resize(static_cast<size_t>(out_size));
  int64_t op = 0;
  while (pos < in_len) {
    const uint8_t tag = in[pos++];
    const int32_t kind = tag & 0x03;
    int64_t length = 0;
    int64_t offset = 0;
    if (kind == 0) {
      length = static_cast<int64_t>(tag >> 2);
      if (length < 60) {
        length += 1;
      } else {
        const int32_t extra = static_cast<int32_t>(length) - 59;
        if (extra < 1 || extra > 4 || pos + extra > in_len) {
          err = "parquet: invalid snappy literal tag";
          return -1;
        }
        uint64_t value = 0;
        for (int32_t i = 0; i < extra; ++i) {
          value |= static_cast<uint64_t>(in[pos + i]) << (8 * i);
        }
        pos += extra;
        if (value > static_cast<uint64_t>(PQ_MAX_ALLOC_BYTES)) {
          err = "parquet: snappy literal is too large";
          return -1;
        }
        length = static_cast<int64_t>(value) + 1;
      }
      if (length < 0 || pos + length > in_len || op + length > out_size) {
        err = "parquet: truncated snappy literal";
        return -1;
      }
      std::memcpy(out.data() + op, in + pos, static_cast<size_t>(length));
      pos += length;
      op += length;
    } else {
      if (kind == 1) {
        if (pos + 1 > in_len) {
          err = "parquet: truncated snappy copy";
          return -1;
        }
        length = ((tag >> 2) & 0x07) + 4;
        offset = (static_cast<int64_t>(tag >> 5) << 8) | static_cast<int64_t>(in[pos]);
        pos += 1;
      } else if (kind == 2) {
        if (pos + 2 > in_len) {
          err = "parquet: truncated snappy copy";
          return -1;
        }
        length = static_cast<int64_t>(tag >> 2) + 1;
        offset = static_cast<int64_t>(in[pos]) | (static_cast<int64_t>(in[pos + 1]) << 8);
        pos += 2;
      } else {
        if (pos + 4 > in_len) {
          err = "parquet: truncated snappy copy";
          return -1;
        }
        length = static_cast<int64_t>(tag >> 2) + 1;
        offset = static_cast<int64_t>(in[pos]) |
                 (static_cast<int64_t>(in[pos + 1]) << 8) |
                 (static_cast<int64_t>(in[pos + 2]) << 16) |
                 (static_cast<int64_t>(in[pos + 3]) << 24);
        pos += 4;
      }
      if (offset <= 0 || offset > op || op + length > out_size) {
        err = "parquet: invalid snappy copy offset";
        return -1;
      }
      uint8_t *dst = out.data();
      for (int64_t i = 0; i < length; ++i) {
        dst[op] = dst[op - offset];
        ++op;
      }
    }
  }
  if (op != out_size) {
    err = "parquet: snappy stream is truncated";
    return -1;
  }
  return 0;
}

// ---------------------------------------------------------------------------
// RLE / bit-packed hybrid decoder (the one used for levels and dictionary
// indices).  Values are packed from the least significant bit of each byte.
// ---------------------------------------------------------------------------
class PqRleBitPackedDecoder
{
public:
  PqRleBitPackedDecoder() {}
  ~PqRleBitPackedDecoder() {}

  void init(const uint8_t *data, int64_t len, int32_t bit_width)
  {
    data_ = data;
    len_ = len;
    pos_ = 0;
    bit_width_ = bit_width;
    rle_count_ = 0;
    rle_value_ = 0;
    bp_values_left_ = 0;
    bp_bit_pos_ = 0;
    ok_ = true;
  }

  bool ok() const { return ok_; }
  int64_t consumed() const { return pos_; }
  int32_t bit_width() const { return bit_width_; }

  bool next(uint32_t &value)
  {
    for (;;) {
      if (rle_count_ > 0) {
        value = rle_value_;
        --rle_count_;
        return true;
      }
      if (bp_values_left_ > 0) {
        if (bit_width_ == 0) {
          value = 0;
        } else if (!read_bits(bp_bit_pos_, bit_width_, value)) {
          return false;
        }
        bp_bit_pos_ += bit_width_;
        --bp_values_left_;
        return true;
      }
      if (!load_run()) {
        return false;
      }
    }
  }

private:
  bool read_bits(int64_t bit_pos, int32_t width, uint32_t &value)
  {
    value = 0;
    const int64_t total_bits = len_ * 8;
    if (width < 0 || bit_pos < 0 || bit_pos + width > total_bits) {
      ok_ = false;
      return false;
    }
    for (int32_t i = 0; i < width; ++i) {
      const int64_t bit = bit_pos + i;
      value |= static_cast<uint32_t>((data_[bit >> 3] >> (bit & 7)) & 1) << i;
    }
    return true;
  }

  bool load_run()
  {
    if (pos_ >= len_) {
      ok_ = false;
      return false;
    }
    // ULEB128 header.
    uint64_t header = 0;
    int32_t shift = 0;
    bool have = false;
    while (pos_ < len_ && shift <= 63) {
      const uint8_t byte = data_[pos_++];
      header |= static_cast<uint64_t>(byte & 0x7f) << shift;
      if ((byte & 0x80) == 0) {
        have = true;
        break;
      }
      shift += 7;
    }
    if (!have) {
      ok_ = false;
      return false;
    }
    if ((header & 1) != 0) {
      const uint64_t groups = header >> 1;
      if (groups == 0) {
        ok_ = false;
        return false;
      }
      int64_t bytes = 0;
      if (bit_width_ > 0) {
        if (groups > static_cast<uint64_t>((len_ - pos_) / bit_width_)) {
          ok_ = false;
          return false;
        }
        bytes = static_cast<int64_t>(groups) * bit_width_;
      }
      if (groups > static_cast<uint64_t>(std::numeric_limits<int64_t>::max() / 8)) {
        ok_ = false;
        return false;
      }
      bp_bit_pos_ = pos_ * 8;
      bp_values_left_ = static_cast<int64_t>(groups) * 8;
      pos_ += bytes;
    } else {
      const uint64_t count = header >> 1;
      if (count == 0) {
        ok_ = false;
        return false;
      }
      const int64_t value_bytes = (bit_width_ + 7) / 8;
      if (pos_ + value_bytes > len_) {
        ok_ = false;
        return false;
      }
      uint32_t value = 0;
      for (int64_t i = 0; i < value_bytes; ++i) {
        value |= static_cast<uint32_t>(data_[pos_ + i]) << (8 * i);
      }
      pos_ += value_bytes;
      rle_count_ = static_cast<int64_t>(count);
      rle_value_ = value;
    }
    return true;
  }

  const uint8_t *data_ = nullptr;
  int64_t len_ = 0;
  int64_t pos_ = 0;
  int32_t bit_width_ = 0;
  int64_t rle_count_ = 0;
  uint32_t rle_value_ = 0;
  int64_t bp_values_left_ = 0;
  int64_t bp_bit_pos_ = 0;
  bool ok_ = false;
};

// Deprecated BIT_PACKED encoding: most significant bit first, no run headers.
bool pq_read_bit_packed_msb(const uint8_t *data,
                            int64_t len,
                            int64_t index,
                            int32_t width,
                            uint32_t &value)
{
  value = 0;
  if (width < 0 || width > 32) {
    return false;
  }
  const int64_t total_bits = len * 8;
  const int64_t start = index * width;
  if (start < 0 || start + width > total_bits) {
    return false;
  }
  for (int32_t i = 0; i < width; ++i) {
    const int64_t bit = start + i;
    const uint32_t bit_value = (data[bit >> 3] >> (7 - (bit & 7))) & 1;
    value = (value << 1) | bit_value;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Date / time / decimal formatting helpers.
// ---------------------------------------------------------------------------
void pq_civil_from_days(int64_t days, int64_t &year, uint32_t &month, uint32_t &day)
{
  const int64_t z = days + 719468;
  const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
  const uint32_t doe = static_cast<uint32_t>(z - era * 146097);
  const uint32_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  const int64_t y = static_cast<int64_t>(yoe) + era * 400;
  const uint32_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  const uint32_t mp = (5 * doy + 2) / 153;
  day = doy - (153 * mp + 2) / 5 + 1;
  month = mp < 10 ? mp + 3 : mp - 9;
  year = y + (month <= 2 ? 1 : 0);
}

void pq_format_date(int64_t days, std::string &out)
{
  int64_t year = 0;
  uint32_t month = 0;
  uint32_t day = 0;
  pq_civil_from_days(days, year, month, day);
  char buffer[48];
  std::snprintf(buffer, sizeof(buffer), "%04lld-%02u-%02u",
                static_cast<long long>(year), month, day);
  out.assign(buffer);
}

void pq_format_datetime_parts(int64_t days,
                              int64_t second_of_day,
                              int64_t fraction,
                              int32_t fraction_digits,
                              std::string &out)
{
  while (second_of_day < 0) {
    second_of_day += 86400;
    --days;
  }
  while (second_of_day >= 86400) {
    second_of_day -= 86400;
    ++days;
  }
  int64_t year = 0;
  uint32_t month = 0;
  uint32_t day = 0;
  pq_civil_from_days(days, year, month, day);
  char buffer[80];
  std::snprintf(buffer, sizeof(buffer), "%04lld-%02u-%02u %02lld:%02lld:%02lld",
                static_cast<long long>(year), month, day,
                static_cast<long long>(second_of_day / 3600),
                static_cast<long long>((second_of_day / 60) % 60),
                static_cast<long long>(second_of_day % 60));
  out.assign(buffer);
  if (fraction != 0) {
    std::snprintf(buffer, sizeof(buffer), ".%0*lld",
                  static_cast<int>(fraction_digits), static_cast<long long>(fraction));
    out += buffer;
  }
}

void pq_format_epoch_units(int64_t value, int64_t units_per_second, std::string &out)
{
  if (units_per_second <= 0) {
    units_per_second = 1000;
  }
  int64_t seconds = value / units_per_second;
  int64_t fraction = value % units_per_second;
  if (fraction < 0) {
    fraction += units_per_second;
    --seconds;
  }
  int64_t days = seconds / 86400;
  int64_t second_of_day = seconds % 86400;
  if (second_of_day < 0) {
    second_of_day += 86400;
    --days;
  }
  int32_t fraction_digits = 3;
  if (units_per_second >= 1000000000LL) {
    fraction_digits = 9;
  } else if (units_per_second >= 1000000LL) {
    fraction_digits = 6;
  }
  pq_format_datetime_parts(days, second_of_day, fraction, fraction_digits, out);
}

void pq_format_int96(const uint8_t *data, std::string &out)
{
  uint64_t nanos = 0;
  for (int32_t i = 0; i < 8; ++i) {
    nanos |= static_cast<uint64_t>(data[i]) << (8 * i);
  }
  const uint32_t raw_julian_day = static_cast<uint32_t>(data[8]) |
                                  (static_cast<uint32_t>(data[9]) << 8) |
                                  (static_cast<uint32_t>(data[10]) << 16) |
                                  (static_cast<uint32_t>(data[11]) << 24);
  // The Julian day is a signed 32 bit value: some writers emit out of range
  // timestamps whose Julian day does not fit into the positive range.
  const int64_t days = static_cast<int64_t>(static_cast<int32_t>(raw_julian_day)) - 2440588;
  const int64_t second_of_day = static_cast<int64_t>(nanos / 1000000000ULL);
  const int64_t fraction = static_cast<int64_t>(nanos % 1000000000ULL);
  pq_format_datetime_parts(days, second_of_day, fraction, 9, out);
}

double pq_decimal_divisor(int32_t scale)
{
  if (scale <= 0) {
    return 1.0;
  }
  if (scale > 18) {
    scale = 18;
  }
  double divisor = 1.0;
  for (int32_t i = 0; i < scale; ++i) {
    divisor *= 10.0;
  }
  return divisor;
}

// Big endian two's complement decimal payload (BYTE_ARRAY / FLBA decimals).
double pq_decimal_from_be_bytes(const uint8_t *data, int64_t len, int32_t scale)
{
  const double divisor = pq_decimal_divisor(scale);
  if (len <= 0) {
    return 0.0;
  }
  if (len <= 8) {
    uint64_t raw = 0;
    for (int64_t i = 0; i < len; ++i) {
      raw = (raw << 8) | data[i];
    }
    int64_t signed_value = static_cast<int64_t>(raw);
    if (len < 8 && (data[0] & 0x80) != 0) {
      signed_value -= static_cast<int64_t>(1) << (len * 8);
    }
    return static_cast<double>(signed_value) / divisor;
  }
  const bool negative = (data[0] & 0x80) != 0;
  long double magnitude = 0.0L;
  for (int64_t i = 0; i < len; ++i) {
    magnitude = magnitude * 256.0L + static_cast<long double>(data[i]);
  }
  if (negative && len <= 32) {
    magnitude -= std::pow(256.0L, static_cast<long double>(len));
  }
  return static_cast<double>(magnitude / static_cast<long double>(divisor));
}

// ---------------------------------------------------------------------------
// Schema model.
// ---------------------------------------------------------------------------
struct PqSchemaElement
{
  int32_t physical_type = -1;
  bool has_physical_type = false;
  int32_t type_length = 0;
  int32_t repetition = -1;
  bool has_repetition = false;
  std::string name;
  int32_t num_children = 0;
  bool has_num_children = false;
  int32_t converted_type = -1;
  bool has_converted_type = false;
  int32_t scale = 0;
  int32_t precision = 0;
  // LogicalType union members that matter for the SQL type mapping.
  bool logical_string = false;
  bool logical_enum = false;
  bool logical_json = false;
  bool logical_date = false;
  bool logical_decimal = false;
  bool logical_timestamp = false;
  bool logical_integer = false;
  int32_t logical_time_unit = 0; // 0 none, 1 millis, 2 micros, 3 nanos
  int32_t logical_int_width = 0;
  bool logical_int_signed = true;
  bool logical_decimal_scale_set = false;
  int32_t logical_decimal_scale = 0;
};

struct PqLeafSchema
{
  std::string name;
  int32_t leaf_index = 0;
  int32_t physical_type = -1;
  int32_t repetition = PQ_REP_REQUIRED;
  int32_t converted_type = -1;
  int32_t type_length = 0;
  int32_t decimal_scale = 0;
  int32_t timestamp_unit = 0; // 0 none, 1 millis, 2 micros, 3 nanos
  bool is_timestamp = false;
  bool is_decimal = false;
  FileColumnType file_type = FILE_COL_VARCHAR;
};

struct PqColumnChunkMeta
{
  int32_t physical_type = -1;
  int32_t codec = -1;
  int64_t num_values = 0;
  int64_t total_compressed_size = 0;
  int64_t total_uncompressed_size = 0;
  int64_t data_page_offset = -1;
  int64_t dictionary_page_offset = -1;
  int64_t file_offset = -1;
};

struct PqRowGroupMeta
{
  std::vector<PqColumnChunkMeta> columns;
  int64_t num_rows = 0;
};

struct PqFileMeta
{
  int32_t version = 0;
  int64_t num_rows = 0;
  std::vector<PqLeafSchema> leaves;
  std::vector<PqRowGroupMeta> row_groups;
};

// ---------------------------------------------------------------------------
// Thrift parsers for the footer metadata.
// ---------------------------------------------------------------------------
bool pq_parse_time_unit(PqThriftReader &reader, int32_t &unit)
{
  int32_t field_id = 0;
  PqStructFieldReader fields(reader);
  for (;;) {
    uint8_t type = 0;
    const int rc = fields.next_field(field_id, type);
    if (rc < 0) {
      return false;
    }
    if (rc == 0) {
      return true;
    }
    if (field_id >= 1 && field_id <= 3 && type == PQ_TT_STRUCT) {
      unit = field_id; // 1 = MILLIS, 2 = MICROS, 3 = NANOS
      if (!reader.skip_struct(1)) {
        return false;
      }
    } else if (!reader.skip_value(type, 0)) {
      return false;
    }
  }
}

bool pq_parse_logical_type(PqThriftReader &reader, PqSchemaElement &element)
{
  int32_t field_id = 0;
  PqStructFieldReader fields(reader);
  for (;;) {
    uint8_t type = 0;
    const int rc = fields.next_field(field_id, type);
    if (rc < 0) {
      return false;
    }
    if (rc == 0) {
      return true;
    }
    if (type != PQ_TT_STRUCT) {
      if (!reader.skip_value(type, 0)) {
        return false;
      }
      continue;
    }
    switch (field_id) {
      case 1: // StringType
        element.logical_string = true;
        if (!reader.skip_struct(1)) {
          return false;
        }
        break;
      case 4: // EnumType
        element.logical_enum = true;
        if (!reader.skip_struct(1)) {
          return false;
        }
        break;
      case 11: // JsonType
        element.logical_json = true;
        if (!reader.skip_struct(1)) {
          return false;
        }
        break;
      case 5: { // DecimalType { 1: scale, 2: precision }
        element.logical_decimal = true;
        int32_t inner_id = 0;
        PqStructFieldReader inner(reader);
        for (;;) {
          uint8_t inner_type = 0;
          const int inner_rc = inner.next_field(inner_id, inner_type);
          if (inner_rc <= 0) {
            if (inner_rc < 0) {
              return false;
            }
            break;
          }
          if (inner_id == 1 && inner_type == PQ_TT_I32) {
            int64_t value = 0;
            if (!reader.read_zigzag(value)) {
              return false;
            }
            element.logical_decimal_scale = static_cast<int32_t>(value);
            element.logical_decimal_scale_set = true;
          } else if (inner_id == 2 && inner_type == PQ_TT_I32) {
            int64_t value = 0;
            if (!reader.read_zigzag(value)) {
              return false;
            }
            element.precision = static_cast<int32_t>(value);
          } else if (!reader.skip_value(inner_type, 1)) {
            return false;
          }
        }
        break;
      }
      case 6: // DateType
        element.logical_date = true;
        if (!reader.skip_struct(1)) {
          return false;
        }
        break;
      case 8: { // TimestampType { 1: isAdjustedToUTC, 2: TimeUnit }
        element.logical_timestamp = true;
        int32_t inner_id = 0;
        PqStructFieldReader inner(reader);
        for (;;) {
          uint8_t inner_type = 0;
          const int inner_rc = inner.next_field(inner_id, inner_type);
          if (inner_rc <= 0) {
            if (inner_rc < 0) {
              return false;
            }
            break;
          }
          if (inner_id == 2 && inner_type == PQ_TT_STRUCT) {
            if (!pq_parse_time_unit(reader, element.logical_time_unit)) {
              return false;
            }
          } else if (!reader.skip_value(inner_type, 1)) {
            return false;
          }
        }
        break;
      }
      case 9: { // IntegerType { 1: bitWidth (byte), 2: isSigned (bool) }
        element.logical_integer = true;
        int32_t inner_id = 0;
        PqStructFieldReader inner(reader);
        for (;;) {
          uint8_t inner_type = 0;
          const int inner_rc = inner.next_field(inner_id, inner_type);
          if (inner_rc <= 0) {
            if (inner_rc < 0) {
              return false;
            }
            break;
          }
          if (inner_id == 1 && inner_type == PQ_TT_BYTE) {
            uint8_t value = 0;
            if (!reader.read_u8(value)) {
              return false;
            }
            element.logical_int_width = value;
          } else if (inner_id == 2 && (inner_type == PQ_TT_TRUE || inner_type == PQ_TT_FALSE)) {
            element.logical_int_signed = (inner_type == PQ_TT_TRUE);
          } else if (!reader.skip_value(inner_type, 1)) {
            return false;
          }
        }
        break;
      }
      default:
        if (!reader.skip_struct(1)) {
          return false;
        }
        break;
    }
  }
}

bool pq_parse_schema_element(PqThriftReader &reader, PqSchemaElement &element)
{
  int32_t field_id = 0;
  PqStructFieldReader fields(reader);
  for (;;) {
    uint8_t type = 0;
    const int rc = fields.next_field(field_id, type);
    if (rc < 0) {
      return false;
    }
    if (rc == 0) {
      return true;
    }
    switch (field_id) {
      case 1:
        if (type != PQ_TT_I32) {
          return false;
        }
        {
          int64_t value = 0;
          if (!reader.read_zigzag(value)) {
            return false;
          }
          element.physical_type = static_cast<int32_t>(value);
          element.has_physical_type = true;
        }
        break;
      case 2:
        if (type != PQ_TT_I32) {
          return false;
        }
        {
          int64_t value = 0;
          if (!reader.read_zigzag(value)) {
            return false;
          }
          element.type_length = static_cast<int32_t>(value);
        }
        break;
      case 3:
        if (type != PQ_TT_I32) {
          return false;
        }
        {
          int64_t value = 0;
          if (!reader.read_zigzag(value)) {
            return false;
          }
          element.repetition = static_cast<int32_t>(value);
          element.has_repetition = true;
        }
        break;
      case 4: {
        if (type != PQ_TT_BINARY) {
          return false;
        }
        const uint8_t *data = nullptr;
        int64_t len = 0;
        if (!reader.read_binary(data, len)) {
          return false;
        }
        element.name.assign(reinterpret_cast<const char *>(data), static_cast<size_t>(len));
        break;
      }
      case 5:
        if (type != PQ_TT_I32) {
          return false;
        }
        {
          int64_t value = 0;
          if (!reader.read_zigzag(value)) {
            return false;
          }
          element.num_children = static_cast<int32_t>(value);
          element.has_num_children = true;
        }
        break;
      case 6:
        if (type != PQ_TT_I32) {
          return false;
        }
        {
          int64_t value = 0;
          if (!reader.read_zigzag(value)) {
            return false;
          }
          element.converted_type = static_cast<int32_t>(value);
          element.has_converted_type = true;
        }
        break;
      case 7:
        if (type != PQ_TT_I32) {
          return false;
        }
        {
          int64_t value = 0;
          if (!reader.read_zigzag(value)) {
            return false;
          }
          element.scale = static_cast<int32_t>(value);
        }
        break;
      case 8:
        if (type != PQ_TT_I32) {
          return false;
        }
        {
          int64_t value = 0;
          if (!reader.read_zigzag(value)) {
            return false;
          }
          element.precision = static_cast<int32_t>(value);
        }
        break;
      case 10:
        if (type != PQ_TT_STRUCT) {
          return false;
        }
        if (!pq_parse_logical_type(reader, element)) {
          return false;
        }
        break;
      default:
        if (!reader.skip_value(type, 0)) {
          return false;
        }
        break;
    }
  }
}

bool pq_parse_column_meta_data(PqThriftReader &reader, PqColumnChunkMeta &chunk)
{
  int32_t field_id = 0;
  PqStructFieldReader fields(reader);
  for (;;) {
    uint8_t type = 0;
    const int rc = fields.next_field(field_id, type);
    if (rc < 0) {
      return false;
    }
    if (rc == 0) {
      return true;
    }
    switch (field_id) {
      case 1: {
        if (type != PQ_TT_I32) {
          return false;
        }
        int64_t value = 0;
        if (!reader.read_zigzag(value)) {
          return false;
        }
        chunk.physical_type = static_cast<int32_t>(value);
        break;
      }
      case 2: { // encodings
        if (type != PQ_TT_LIST) {
          return false;
        }
        uint8_t elem_type = 0;
        int64_t count = 0;
        if (!reader.read_list_header(elem_type, count)) {
          return false;
        }
        for (int64_t i = 0; i < count; ++i) {
          if (!reader.skip_list_element(elem_type, 1)) {
            return false;
          }
        }
        break;
      }
      case 3: { // path_in_schema
        if (type != PQ_TT_LIST) {
          return false;
        }
        uint8_t elem_type = 0;
        int64_t count = 0;
        if (!reader.read_list_header(elem_type, count)) {
          return false;
        }
        for (int64_t i = 0; i < count; ++i) {
          if (!reader.skip_list_element(elem_type, 1)) {
            return false;
          }
        }
        break;
      }
      case 4:
        if (type != PQ_TT_I32) {
          return false;
        }
        {
          int64_t value = 0;
          if (!reader.read_zigzag(value)) {
            return false;
          }
          chunk.codec = static_cast<int32_t>(value);
        }
        break;
      case 5:
        if (type != PQ_TT_I64) {
          return false;
        }
        if (!reader.read_zigzag(chunk.num_values)) {
          return false;
        }
        break;
      case 6:
        if (type != PQ_TT_I64) {
          return false;
        }
        if (!reader.read_zigzag(chunk.total_uncompressed_size)) {
          return false;
        }
        break;
      case 7:
        if (type != PQ_TT_I64) {
          return false;
        }
        if (!reader.read_zigzag(chunk.total_compressed_size)) {
          return false;
        }
        break;
      case 9:
        if (type != PQ_TT_I64) {
          return false;
        }
        if (!reader.read_zigzag(chunk.data_page_offset)) {
          return false;
        }
        break;
      case 11:
        if (type != PQ_TT_I64) {
          return false;
        }
        if (!reader.read_zigzag(chunk.dictionary_page_offset)) {
          return false;
        }
        break;
      default:
        if (!reader.skip_value(type, 0)) {
          return false;
        }
        break;
    }
  }
}

bool pq_parse_column_chunk(PqThriftReader &reader, PqColumnChunkMeta &chunk)
{
  int32_t field_id = 0;
  bool has_file_path = false;
  PqStructFieldReader fields(reader);
  for (;;) {
    uint8_t type = 0;
    const int rc = fields.next_field(field_id, type);
    if (rc < 0) {
      return false;
    }
    if (rc == 0) {
      return !has_file_path;
    }
    switch (field_id) {
      case 1: {
        if (type != PQ_TT_BINARY) {
          return false;
        }
        const uint8_t *data = nullptr;
        int64_t len = 0;
        if (!reader.read_binary(data, len)) {
          return false;
        }
        has_file_path = len > 0;
        break;
      }
      case 2:
        if (type != PQ_TT_I64) {
          return false;
        }
        if (!reader.read_zigzag(chunk.file_offset)) {
          return false;
        }
        break;
      case 3:
        if (type != PQ_TT_STRUCT) {
          return false;
        }
        if (!pq_parse_column_meta_data(reader, chunk)) {
          return false;
        }
        break;
      default:
        if (!reader.skip_value(type, 0)) {
          return false;
        }
        break;
    }
  }
}

bool pq_parse_row_group(PqThriftReader &reader, PqRowGroupMeta &row_group)
{
  int32_t field_id = 0;
  PqStructFieldReader fields(reader);
  for (;;) {
    uint8_t type = 0;
    const int rc = fields.next_field(field_id, type);
    if (rc < 0) {
      return false;
    }
    if (rc == 0) {
      return true;
    }
    if (field_id == 1) {
      if (type != PQ_TT_LIST) {
        return false;
      }
      uint8_t elem_type = 0;
      int64_t count = 0;
      if (!reader.read_list_header(elem_type, count)) {
        return false;
      }
      for (int64_t i = 0; i < count; ++i) {
        PqColumnChunkMeta chunk;
        if (!pq_parse_column_chunk(reader, chunk)) {
          return false;
        }
        row_group.columns.push_back(chunk);
      }
    } else if (field_id == 3) {
      if (type != PQ_TT_I64) {
        return false;
      }
      if (!reader.read_zigzag(row_group.num_rows)) {
        return false;
      }
    } else if (!reader.skip_value(type, 0)) {
      return false;
    }
  }
}

bool pq_parse_file_meta_data(const uint8_t *data,
                             int64_t len,
                             PqFileMeta &meta,
                             std::string &err)
{
  PqThriftReader reader(data, len);
  std::vector<PqSchemaElement> elements;
  bool has_schema = false;
  bool has_row_groups = false;
  int32_t field_id = 0;
  uint8_t malformed_type = 0;
  PqStructFieldReader fields(reader);
  for (;;) {
    uint8_t type = 0;
    const int rc = fields.next_field(field_id, type);
    if (rc < 0) {
      err = "parquet: malformed footer metadata";
      return false;
    }
    if (rc == 0) {
      break;
    }
    switch (field_id) {
      case 1:
        if (type == PQ_TT_I32) {
          int64_t value = 0;
          if (!reader.read_zigzag(value)) {
            err = "parquet: malformed footer metadata";
            return false;
          }
          meta.version = static_cast<int32_t>(value);
        } else {
          malformed_type = type;
        }
        break;
      case 2: {
        if (type != PQ_TT_LIST) {
          malformed_type = type;
          break;
        }
        uint8_t elem_type = 0;
        int64_t count = 0;
        if (!reader.read_list_header(elem_type, count)) {
          err = "parquet: malformed schema list";
          return false;
        }
        for (int64_t i = 0; i < count; ++i) {
          PqSchemaElement element;
          if (!pq_parse_schema_element(reader, element)) {
            err = "parquet: malformed schema element";
            return false;
          }
          elements.push_back(element);
        }
        has_schema = true;
        break;
      }
      case 3:
        if (type == PQ_TT_I64) {
          if (!reader.read_zigzag(meta.num_rows)) {
            err = "parquet: malformed footer metadata";
            return false;
          }
        } else {
          malformed_type = type;
        }
        break;
      case 4: {
        if (type != PQ_TT_LIST) {
          malformed_type = type;
          break;
        }
        uint8_t elem_type = 0;
        int64_t count = 0;
        if (!reader.read_list_header(elem_type, count)) {
          err = "parquet: malformed row group list";
          return false;
        }
        for (int64_t i = 0; i < count; ++i) {
          PqRowGroupMeta row_group;
          if (!pq_parse_row_group(reader, row_group)) {
            err = "parquet: malformed row group";
            return false;
          }
          meta.row_groups.push_back(row_group);
        }
        has_row_groups = true;
        break;
      }
      default:
        if (!reader.skip_value(type, 0)) {
          err = "parquet: malformed footer metadata";
          return false;
        }
        break;
    }
    if (malformed_type != 0) {
      err = "parquet: unexpected field type in footer metadata";
      return false;
    }
  }
  if (!has_schema) {
    err = "parquet: footer has no schema";
    return false;
  }
  if (!has_row_groups) {
    err = "parquet: footer has no row groups";
    return false;
  }
  if (meta.num_rows < 0) {
    err = "parquet: negative row count in footer";
    return false;
  }
  // Map the flattened schema to leaf columns.
  if (elements.empty()) {
    err = "parquet: empty schema";
    return false;
  }
  const PqSchemaElement &root = elements[0];
  if (root.has_physical_type) {
    err = "parquet: schema root must be a group";
    return false;
  }
  int64_t child_count = root.has_num_children ? root.num_children
                                              : static_cast<int64_t>(elements.size()) - 1;
  if (child_count < 0 || child_count > static_cast<int64_t>(elements.size()) - 1) {
    err = "parquet: schema child count is inconsistent";
    return false;
  }
  if (child_count != static_cast<int64_t>(elements.size()) - 1) {
    err = "parquet: nested schema is not supported";
    return false;
  }
  meta.leaves.clear();
  for (int64_t i = 0; i < child_count; ++i) {
    const PqSchemaElement &element = elements[static_cast<size_t>(1 + i)];
    if (!element.has_physical_type) {
      err = "parquet: nested schema is not supported";
      return false;
    }
    if (element.has_num_children && element.num_children > 0) {
      err = "parquet: nested schema is not supported";
      return false;
    }
    if (!element.has_repetition) {
      err = "parquet: schema element has no repetition type";
      return false;
    }
    if (element.repetition == PQ_REP_REPEATED) {
      err = "parquet: repeated columns are not supported";
      return false;
    }
    if (element.repetition != PQ_REP_REQUIRED && element.repetition != PQ_REP_OPTIONAL) {
      err = "parquet: invalid repetition type in schema";
      return false;
    }
    if (element.physical_type < PQ_PHYS_BOOLEAN || element.physical_type > PQ_PHYS_FIXED_LEN_BYTE_ARRAY) {
      err = "parquet: invalid physical type in schema";
      return false;
    }
    PqLeafSchema leaf;
    leaf.name = element.name;
    leaf.leaf_index = static_cast<int32_t>(i);
    leaf.physical_type = element.physical_type;
    leaf.repetition = element.repetition;
    leaf.converted_type = element.has_converted_type ? element.converted_type : -1;
    leaf.type_length = element.type_length;
    leaf.decimal_scale = element.scale;
    leaf.is_decimal = (leaf.converted_type == PQ_CT_DECIMAL) || element.logical_decimal;
    if (element.logical_decimal && element.logical_decimal_scale_set) {
      leaf.decimal_scale = element.logical_decimal_scale;
    }
    if (leaf.is_decimal && leaf.decimal_scale < 0) {
      leaf.decimal_scale = 0;
    }
    if (element.logical_timestamp) {
      leaf.is_timestamp = true;
      leaf.timestamp_unit = element.logical_time_unit;
    } else if (leaf.converted_type == PQ_CT_TIMESTAMP_MILLIS) {
      leaf.is_timestamp = true;
      leaf.timestamp_unit = 1;
    } else if (leaf.converted_type == PQ_CT_TIMESTAMP_MICROS) {
      leaf.is_timestamp = true;
      leaf.timestamp_unit = 2;
    }
    const bool is_date = (leaf.converted_type == PQ_CT_DATE) || element.logical_date;
    switch (leaf.physical_type) {
      case PQ_PHYS_BOOLEAN:
        leaf.file_type = FILE_COL_BOOLEAN;
        break;
      case PQ_PHYS_INT32:
      case PQ_PHYS_INT64:
        if (leaf.is_decimal) {
          leaf.file_type = FILE_COL_DOUBLE;
        } else if (is_date) {
          leaf.file_type = FILE_COL_DATE;
        } else if (leaf.is_timestamp) {
          leaf.file_type = FILE_COL_DATETIME;
        } else {
          leaf.file_type = FILE_COL_BIGINT;
        }
        break;
      case PQ_PHYS_INT96:
        leaf.file_type = FILE_COL_DATETIME;
        break;
      case PQ_PHYS_FLOAT:
      case PQ_PHYS_DOUBLE:
        leaf.file_type = FILE_COL_DOUBLE;
        break;
      case PQ_PHYS_BYTE_ARRAY:
        leaf.file_type = leaf.is_decimal ? FILE_COL_DOUBLE : FILE_COL_VARCHAR;
        break;
      case PQ_PHYS_FIXED_LEN_BYTE_ARRAY:
        if (leaf.type_length <= 0) {
          err = "parquet: FIXED_LEN_BYTE_ARRAY column has no type length";
          return false;
        }
        leaf.file_type = leaf.is_decimal ? FILE_COL_DOUBLE : FILE_COL_VARCHAR;
        break;
      default:
        err = "parquet: invalid physical type in schema";
        return false;
    }
    meta.leaves.push_back(leaf);
  }
  if (meta.leaves.empty()) {
    err = "parquet: schema has no columns";
    return false;
  }
  // Every row group must describe every leaf column.
  for (size_t i = 0; i < meta.row_groups.size(); ++i) {
    PqRowGroupMeta &row_group = meta.row_groups[i];
    if (row_group.num_rows < 0) {
      err = "parquet: negative row count in row group";
      return false;
    }
    if (row_group.columns.size() < meta.leaves.size()) {
      err = "parquet: row group does not contain all columns";
      return false;
    }
    for (size_t j = 0; j < meta.leaves.size(); ++j) {
      if (row_group.columns[j].num_values < 0) {
        err = "parquet: negative value count in column chunk";
        return false;
      }
    }
  }
  return true;
}

// ---------------------------------------------------------------------------
// Little endian loads.
// ---------------------------------------------------------------------------
uint32_t pq_load_u32(const uint8_t *data)
{
  return static_cast<uint32_t>(data[0]) |
         (static_cast<uint32_t>(data[1]) << 8) |
         (static_cast<uint32_t>(data[2]) << 16) |
         (static_cast<uint32_t>(data[3]) << 24);
}

int32_t pq_load_i32(const uint8_t *data)
{
  return static_cast<int32_t>(pq_load_u32(data));
}

int64_t pq_load_i64(const uint8_t *data)
{
  uint64_t value = 0;
  for (int32_t i = 0; i < 8; ++i) {
    value |= static_cast<uint64_t>(data[i]) << (8 * i);
  }
  return static_cast<int64_t>(value);
}

// ---------------------------------------------------------------------------
// Random access file wrapper.
// ---------------------------------------------------------------------------
class PqFileReader
{
public:
  PqFileReader() {}
  ~PqFileReader() { close(); }

  int open(const std::string &path, std::string &err)
  {
    close();
    in_.open(path.c_str(), std::ios::in | std::ios::binary);
    if (!in_.is_open()) {
      err = "parquet: cannot open file '" + path + "'";
      return -1;
    }
    in_.seekg(0, std::ios::end);
    const std::streamoff end = in_.tellg();
    if (end < 0) {
      err = "parquet: cannot determine the size of file '" + path + "'";
      close();
      return -1;
    }
    size_ = static_cast<int64_t>(end);
    path_ = path;
    return 0;
  }

  void close()
  {
    if (in_.is_open()) {
      in_.close();
    }
    size_ = 0;
    path_.clear();
  }

  int64_t size() const { return size_; }
  const std::string &path() const { return path_; }

  int read_at(int64_t offset, int64_t len, std::vector<uint8_t> &out, std::string &err) const
  {
    out.clear();
    if (len < 0 || len > PQ_MAX_ALLOC_BYTES) {
      err = "parquet: read size out of range";
      return -1;
    }
    if (len == 0) {
      return 0;
    }
    if (offset < 0 || offset > size_ || len > size_ - offset) {
      err = "parquet: read past the end of the file";
      return -1;
    }
    out.resize(static_cast<size_t>(len));
    in_.clear();
    in_.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    if (!in_.good()) {
      err = "parquet: failed to seek in file";
      return -1;
    }
    in_.read(reinterpret_cast<char *>(out.data()), static_cast<std::streamsize>(len));
    if (in_.gcount() != static_cast<std::streamsize>(len)) {
      err = "parquet: short read";
      return -1;
    }
    return 0;
  }

private:
  mutable std::ifstream in_;
  int64_t size_ = 0;
  std::string path_;
};

// ---------------------------------------------------------------------------
// Conversion of one decoded physical value into a SQL friendly cell.
// ---------------------------------------------------------------------------
void pq_reset_cell(Cell &cell, FileColumnType type)
{
  cell.is_null = false;
  cell.type = type;
  cell.int_value = 0;
  cell.double_value = 0;
  cell.bool_value = false;
  cell.str_value.clear();
}

void pq_make_int_cell(const PqLeafSchema &leaf, int64_t raw, Cell &cell)
{
  pq_reset_cell(cell, leaf.file_type);
  switch (leaf.file_type) {
    case FILE_COL_DATE:
      pq_format_date(raw, cell.str_value);
      break;
    case FILE_COL_DATETIME: {
      int64_t units_per_second = 1000;
      if (leaf.timestamp_unit == 2) {
        units_per_second = 1000000;
      } else if (leaf.timestamp_unit == 3) {
        units_per_second = 1000000000LL;
      } else if (leaf.timestamp_unit == 1) {
        units_per_second = 1000;
      } else if (leaf.converted_type == PQ_CT_TIMESTAMP_MICROS) {
        units_per_second = 1000000;
      }
      pq_format_epoch_units(raw, units_per_second, cell.str_value);
      break;
    }
    case FILE_COL_DOUBLE:
      cell.double_value = static_cast<double>(raw) / pq_decimal_divisor(leaf.decimal_scale);
      break;
    default:
      cell.int_value = raw;
      break;
  }
}

void pq_make_double_cell(const PqLeafSchema &leaf, double raw, Cell &cell)
{
  pq_reset_cell(cell, leaf.file_type);
  cell.double_value = raw;
}

void pq_make_bytes_cell(const PqLeafSchema &leaf,
                        const uint8_t *data,
                        int64_t len,
                        Cell &cell)
{
  pq_reset_cell(cell, leaf.file_type);
  if (leaf.physical_type == PQ_PHYS_INT96) {
    if (len != 12) {
      cell.str_value.clear();
      return;
    }
    pq_format_int96(data, cell.str_value);
  } else if (leaf.file_type == FILE_COL_DOUBLE) {
    cell.double_value = pq_decimal_from_be_bytes(data, len, leaf.decimal_scale);
  } else {
    cell.str_value.assign(reinterpret_cast<const char *>(data), static_cast<size_t>(len));
  }
}

// ---------------------------------------------------------------------------
// Pull based value iterator over the value section of one data page.
// ---------------------------------------------------------------------------
class PqValueIterator
{
public:
  PqValueIterator() {}
  ~PqValueIterator() {}

  void init_plain(const PqLeafSchema &leaf, const uint8_t *data, int64_t len)
  {
    leaf_ = &leaf;
    data_ = data;
    len_ = len;
    pos_ = 0;
    bool_index_ = 0;
    dict_mode_ = false;
    dict_ = nullptr;
    error_.clear();
  }

  void init_dict(const PqLeafSchema &leaf,
                 const std::vector<Cell> &dict,
                 const uint8_t *data,
                 int64_t len,
                 int32_t bit_width)
  {
    leaf_ = &leaf;
    dict_ = &dict;
    data_ = data;
    len_ = len;
    pos_ = 0;
    bool_index_ = 0;
    dict_mode_ = true;
    decoder_.init(data, len, bit_width);
    error_.clear();
  }

  const std::string &error() const { return error_; }

  bool next(Cell &cell)
  {
    if (dict_mode_) {
      uint32_t index = 0;
      if (!decoder_.next(index)) {
        error_ = "parquet: malformed dictionary index stream";
        return false;
      }
      if (static_cast<size_t>(index) >= dict_->size()) {
        error_ = "parquet: dictionary index out of range";
        return false;
      }
      cell = (*dict_)[static_cast<size_t>(index)];
      return true;
    }
    return read_plain(cell);
  }

private:
  bool read_plain(Cell &cell)
  {
    if (leaf_ == nullptr) {
      error_ = "parquet: value iterator is not initialized";
      return false;
    }
    switch (leaf_->physical_type) {
      case PQ_PHYS_BOOLEAN: {
        if ((bool_index_ >> 3) >= len_) {
          error_ = "parquet: truncated boolean values";
          return false;
        }
        const uint8_t byte = data_[bool_index_ >> 3];
        const bool value = ((byte >> (bool_index_ & 7)) & 1) != 0;
        ++bool_index_;
        pq_reset_cell(cell, FILE_COL_BOOLEAN);
        cell.bool_value = value;
        return true;
      }
      case PQ_PHYS_INT32: {
        if (pos_ + 4 > len_) {
          error_ = "parquet: truncated INT32 values";
          return false;
        }
        const int32_t value = pq_load_i32(data_ + pos_);
        pos_ += 4;
        pq_make_int_cell(*leaf_, value, cell);
        return true;
      }
      case PQ_PHYS_INT64: {
        if (pos_ + 8 > len_) {
          error_ = "parquet: truncated INT64 values";
          return false;
        }
        const int64_t value = pq_load_i64(data_ + pos_);
        pos_ += 8;
        pq_make_int_cell(*leaf_, value, cell);
        return true;
      }
      case PQ_PHYS_INT96: {
        if (pos_ + 12 > len_) {
          error_ = "parquet: truncated INT96 values";
          return false;
        }
        pq_make_bytes_cell(*leaf_, data_ + pos_, 12, cell);
        pos_ += 12;
        return true;
      }
      case PQ_PHYS_FLOAT: {
        if (pos_ + 4 > len_) {
          error_ = "parquet: truncated FLOAT values";
          return false;
        }
        const uint32_t bits = pq_load_u32(data_ + pos_);
        pos_ += 4;
        float value = 0.0f;
        std::memcpy(&value, &bits, sizeof(value));
        pq_make_double_cell(*leaf_, static_cast<double>(value), cell);
        return true;
      }
      case PQ_PHYS_DOUBLE: {
        if (pos_ + 8 > len_) {
          error_ = "parquet: truncated DOUBLE values";
          return false;
        }
        const uint64_t bits = static_cast<uint64_t>(pq_load_i64(data_ + pos_));
        pos_ += 8;
        double value = 0.0;
        std::memcpy(&value, &bits, sizeof(value));
        pq_make_double_cell(*leaf_, value, cell);
        return true;
      }
      case PQ_PHYS_BYTE_ARRAY: {
        if (pos_ + 4 > len_) {
          error_ = "parquet: truncated BYTE_ARRAY length";
          return false;
        }
        const int64_t value_len = pq_load_i32(data_ + pos_);
        pos_ += 4;
        if (value_len < 0 || value_len > len_ - pos_) {
          error_ = "parquet: invalid BYTE_ARRAY length";
          return false;
        }
        pq_make_bytes_cell(*leaf_, data_ + pos_, value_len, cell);
        pos_ += value_len;
        return true;
      }
      case PQ_PHYS_FIXED_LEN_BYTE_ARRAY: {
        const int64_t value_len = leaf_->type_length;
        if (value_len <= 0 || value_len > len_ - pos_) {
          error_ = "parquet: truncated FIXED_LEN_BYTE_ARRAY values";
          return false;
        }
        pq_make_bytes_cell(*leaf_, data_ + pos_, value_len, cell);
        pos_ += value_len;
        return true;
      }
      default:
        error_ = "parquet: unsupported physical type";
        return false;
    }
  }

  const PqLeafSchema *leaf_ = nullptr;
  const uint8_t *data_ = nullptr;
  int64_t len_ = 0;
  int64_t pos_ = 0;
  int64_t bool_index_ = 0;
  bool dict_mode_ = false;
  const std::vector<Cell> *dict_ = nullptr;
  PqRleBitPackedDecoder decoder_;
  std::string error_;
};

// ---------------------------------------------------------------------------
// Page headers.
// ---------------------------------------------------------------------------
struct PqPageHeader
{
  int32_t type = -1;
  int32_t uncompressed_page_size = 0;
  int32_t compressed_page_size = 0;
  // DataPageHeader
  bool has_data_header = false;
  int32_t num_values = 0;
  int32_t encoding = -1;
  int32_t def_level_encoding = -1;
  int32_t rep_level_encoding = -1;
  // DictionaryPageHeader
  bool has_dict_header = false;
  int32_t dict_num_values = 0;
  int32_t dict_encoding = -1;
  // DataPageHeaderV2
  bool has_v2_header = false;
  int32_t v2_num_values = 0;
  int32_t v2_num_nulls = 0;
  int32_t v2_num_rows = 0;
  int32_t v2_encoding = -1;
  int32_t v2_def_len = 0;
  int32_t v2_rep_len = 0;
  bool v2_is_compressed = true;
};

bool pq_parse_data_page_header(PqThriftReader &reader, PqPageHeader &header)
{
  int32_t field_id = 0;
  PqStructFieldReader fields(reader);
  for (;;) {
    uint8_t type = 0;
    const int rc = fields.next_field(field_id, type);
    if (rc < 0) {
      return false;
    }
    if (rc == 0) {
      header.has_data_header = true;
      return true;
    }
    if (field_id >= 1 && field_id <= 4 && type == PQ_TT_I32) {
      int64_t value = 0;
      if (!reader.read_zigzag(value)) {
        return false;
      }
      switch (field_id) {
        case 1:
          header.num_values = static_cast<int32_t>(value);
          break;
        case 2:
          header.encoding = static_cast<int32_t>(value);
          break;
        case 3:
          header.def_level_encoding = static_cast<int32_t>(value);
          break;
        default:
          header.rep_level_encoding = static_cast<int32_t>(value);
          break;
      }
    } else if (!reader.skip_value(type, 0)) {
      return false;
    }
  }
}

bool pq_parse_dictionary_page_header(PqThriftReader &reader, PqPageHeader &header)
{
  int32_t field_id = 0;
  PqStructFieldReader fields(reader);
  for (;;) {
    uint8_t type = 0;
    const int rc = fields.next_field(field_id, type);
    if (rc < 0) {
      return false;
    }
    if (rc == 0) {
      header.has_dict_header = true;
      return true;
    }
    if (field_id == 1 && type == PQ_TT_I32) {
      int64_t value = 0;
      if (!reader.read_zigzag(value)) {
        return false;
      }
      header.dict_num_values = static_cast<int32_t>(value);
    } else if (field_id == 2 && type == PQ_TT_I32) {
      int64_t value = 0;
      if (!reader.read_zigzag(value)) {
        return false;
      }
      header.dict_encoding = static_cast<int32_t>(value);
    } else if (!reader.skip_value(type, 0)) {
      return false;
    }
  }
}

bool pq_parse_data_page_header_v2(PqThriftReader &reader, PqPageHeader &header)
{
  int32_t field_id = 0;
  PqStructFieldReader fields(reader);
  for (;;) {
    uint8_t type = 0;
    const int rc = fields.next_field(field_id, type);
    if (rc < 0) {
      return false;
    }
    if (rc == 0) {
      header.has_v2_header = true;
      return true;
    }
    if (field_id == 7) {
      if (type == PQ_TT_TRUE || type == PQ_TT_FALSE) {
        header.v2_is_compressed = (type == PQ_TT_TRUE);
      } else if (!reader.skip_value(type, 0)) {
        return false;
      }
    } else if (field_id >= 1 && field_id <= 6 && type == PQ_TT_I32) {
      int64_t value = 0;
      if (!reader.read_zigzag(value)) {
        return false;
      }
      switch (field_id) {
        case 1:
          header.v2_num_values = static_cast<int32_t>(value);
          break;
        case 2:
          header.v2_num_nulls = static_cast<int32_t>(value);
          break;
        case 3:
          header.v2_num_rows = static_cast<int32_t>(value);
          break;
        case 4:
          header.v2_encoding = static_cast<int32_t>(value);
          break;
        case 5:
          header.v2_def_len = static_cast<int32_t>(value);
          break;
        default:
          header.v2_rep_len = static_cast<int32_t>(value);
          break;
      }
    } else if (!reader.skip_value(type, 0)) {
      return false;
    }
  }
}

bool pq_parse_page_header(PqThriftReader &reader, PqPageHeader &header)
{
  int32_t field_id = 0;
  PqStructFieldReader fields(reader);
  for (;;) {
    uint8_t type = 0;
    const int rc = fields.next_field(field_id, type);
    if (rc < 0) {
      return false;
    }
    if (rc == 0) {
      return true;
    }
    if (field_id == 1 && type == PQ_TT_I32) {
      int64_t value = 0;
      if (!reader.read_zigzag(value)) {
        return false;
      }
      header.type = static_cast<int32_t>(value);
    } else if (field_id == 2 && type == PQ_TT_I32) {
      int64_t value = 0;
      if (!reader.read_zigzag(value)) {
        return false;
      }
      header.uncompressed_page_size = static_cast<int32_t>(value);
    } else if (field_id == 3 && type == PQ_TT_I32) {
      int64_t value = 0;
      if (!reader.read_zigzag(value)) {
        return false;
      }
      header.compressed_page_size = static_cast<int32_t>(value);
    } else if (field_id == 5 && type == PQ_TT_STRUCT) {
      if (!pq_parse_data_page_header(reader, header)) {
        return false;
      }
    } else if (field_id == 7 && type == PQ_TT_STRUCT) {
      if (!pq_parse_dictionary_page_header(reader, header)) {
        return false;
      }
    } else if (field_id == 8 && type == PQ_TT_STRUCT) {
      if (!pq_parse_data_page_header_v2(reader, header)) {
        return false;
      }
    } else if (!reader.skip_value(type, 0)) {
      return false;
    }
  }
}

int pq_read_page_header(const PqFileReader &file,
                        int64_t offset,
                        PqPageHeader &header,
                        int64_t &header_len,
                        std::string &err)
{
  static const int64_t kReadSizes[] = {8192, 65536, PQ_MAX_PAGE_HEADER_BYTES};
  const int64_t available = file.size() - offset;
  if (offset < 0 || available <= 0) {
    err = "parquet: page header offset out of range";
    return -1;
  }
  for (size_t attempt = 0; attempt < sizeof(kReadSizes) / sizeof(kReadSizes[0]); ++attempt) {
    int64_t len = kReadSizes[attempt];
    if (len > available) {
      len = available;
    }
    std::vector<uint8_t> buffer;
    if (file.read_at(offset, len, buffer, err) != 0) {
      return -1;
    }
    PqThriftReader reader(buffer.data(), len);
    header = PqPageHeader();
    if (pq_parse_page_header(reader, header)) {
      if (reader.consumed() <= 0) {
        err = "parquet: empty page header";
        return -1;
      }
      header_len = reader.consumed();
      return 0;
    }
    if (len == available || attempt + 1 == sizeof(kReadSizes) / sizeof(kReadSizes[0])) {
      break;
    }
  }
  err = "parquet: malformed page header";
  return -1;
}

// ---------------------------------------------------------------------------
// One projected column's streaming state.
// ---------------------------------------------------------------------------
struct PqColumnState
{
  int32_t leaf_index = 0;
  const PqLeafSchema *leaf = nullptr;
  const PqColumnChunkMeta *chunk = nullptr;
  FileColumnType file_type = FILE_COL_VARCHAR;
  int32_t max_def_level = 0;
  int64_t chunk_num_values = 0;
  int64_t values_seen = 0;
  int64_t next_offset = 0;
  int64_t end_offset = 0;
  bool done = false;
  bool has_dictionary = false;
  std::vector<Cell> dictionary;
  std::vector<Cell> values;
  size_t head = 0;

  int64_t available() const { return static_cast<int64_t>(values.size() - head); }

  void consume(int64_t count)
  {
    if (count <= 0) {
      return;
    }
    head += static_cast<size_t>(count);
    if (head >= values.size()) {
      values.clear();
      head = 0;
    } else if (head >= 4096 && head * 2 >= values.size()) {
      values.erase(values.begin(), values.begin() + static_cast<std::ptrdiff_t>(head));
      head = 0;
    }
  }
};

// ---------------------------------------------------------------------------
// Decompression.
// ---------------------------------------------------------------------------
int pq_decompress_page(int32_t codec,
                       const uint8_t *data,
                       int64_t data_len,
                       int64_t expected_len,
                       std::vector<uint8_t> &out,
                       std::string &err)
{
  if (codec == PQ_CODEC_UNCOMPRESSED) {
    if (expected_len >= 0 && data_len < expected_len) {
      err = "parquet: page body is truncated";
      return -1;
    }
    const int64_t size = (expected_len >= 0 && expected_len <= data_len) ? expected_len : data_len;
    out.assign(data, data + size);
    return 0;
  }
  if (codec == PQ_CODEC_SNAPPY) {
    return pq_snappy_uncompress(data, data_len, expected_len, out, err);
  }
  err = "parquet: unsupported compression codec " + std::to_string(codec);
  return -1;
}

// ---------------------------------------------------------------------------
// Level decoding.
// ---------------------------------------------------------------------------
int pq_decode_levels_rle(const uint8_t *data,
                         int64_t len,
                         int64_t num_values,
                         int32_t max_level,
                         std::vector<uint32_t> &levels,
                         std::string &err)
{
  if (num_values < 0 || num_values > PQ_MAX_PAGE_VALUES) {
    err = "parquet: data page has too many values";
    return -1;
  }
  levels.assign(static_cast<size_t>(num_values), 0);
  if (max_level <= 0) {
    return 0;
  }
  const int32_t width = pq_bit_width(max_level);
  PqRleBitPackedDecoder decoder;
  decoder.init(data, len, width);
  for (int64_t i = 0; i < num_values; ++i) {
    uint32_t level = 0;
    if (!decoder.next(level)) {
      err = "parquet: malformed level stream";
      return -1;
    }
    if (level > static_cast<uint32_t>(max_level)) {
      err = "parquet: level value out of range";
      return -1;
    }
    levels[static_cast<size_t>(i)] = level;
  }
  return 0;
}

int pq_decode_levels_v1(const uint8_t *page,
                        int64_t page_len,
                        int64_t &pos,
                        int32_t level_encoding,
                        int64_t num_values,
                        int32_t max_level,
                        std::vector<uint32_t> &levels,
                        std::string &err)
{
  levels.clear();
  if (num_values < 0 || num_values > PQ_MAX_PAGE_VALUES) {
    err = "parquet: data page has too many values";
    return -1;
  }
  if (max_level <= 0) {
    return 0;
  }
  const int32_t width = pq_bit_width(max_level);
  if (level_encoding == PQ_ENC_RLE) {
    if (pos + 4 > page_len) {
      err = "parquet: truncated level stream";
      return -1;
    }
    const int64_t len = static_cast<int64_t>(pq_load_u32(page + pos));
    pos += 4;
    if (len < 0 || len > page_len - pos) {
      err = "parquet: invalid level stream length";
      return -1;
    }
    const int rc = pq_decode_levels_rle(page + pos, len, num_values, max_level, levels, err);
    pos += len;
    return rc;
  }
  if (level_encoding == PQ_ENC_BIT_PACKED) {
    int64_t bits = 0;
    if (!pq_checked_mul(num_values, width, bits)) {
      err = "parquet: level stream is too large";
      return -1;
    }
    const int64_t bytes = (bits + 7) / 8;
    if (bytes > page_len - pos) {
      err = "parquet: truncated bit-packed level stream";
      return -1;
    }
    levels.assign(static_cast<size_t>(num_values > 0 ? num_values : 0), 0);
    for (int64_t i = 0; i < num_values; ++i) {
      uint32_t level = 0;
      if (!pq_read_bit_packed_msb(page + pos, bytes, i, width, level) ||
          level > static_cast<uint32_t>(max_level)) {
        err = "parquet: malformed bit-packed level stream";
        return -1;
      }
      levels[static_cast<size_t>(i)] = level;
    }
    pos += bytes;
    return 0;
  }
  err = "parquet: unsupported definition level encoding " + std::to_string(level_encoding);
  return -1;
}

// ---------------------------------------------------------------------------
// Value decoding for one data page.  Nulls are interleaved with the decoded
// values so that every column produces one entry per row slot.
// ---------------------------------------------------------------------------
void pq_append_null_cell(PqColumnState &state)
{
  Cell cell;
  cell.is_null = true;
  cell.type = state.file_type;
  state.values.push_back(std::move(cell));
}

int pq_decode_values(PqColumnState &state,
                     int32_t encoding,
                     const std::vector<uint32_t> &def_levels,
                     int64_t num_values,
                     const uint8_t *values_data,
                     int64_t values_len,
                     std::string &err)
{
  if (num_values < 0 || num_values > PQ_MAX_PAGE_VALUES) {
    err = "parquet: data page has too many values";
    return -1;
  }
  if (num_values > state.chunk_num_values - state.values_seen) {
    err = "parquet: column chunk value count mismatch";
    return -1;
  }
  if (!def_levels.empty() && static_cast<int64_t>(def_levels.size()) < num_values) {
    err = "parquet: level count mismatch";
    return -1;
  }
  const PqLeafSchema &leaf = *state.leaf;
  const int32_t max_def_level = state.max_def_level;
  const bool has_levels = max_def_level > 0;

  if (encoding == PQ_ENC_PLAIN) {
    PqValueIterator iterator;
    iterator.init_plain(leaf, values_data, values_len);
    for (int64_t i = 0; i < num_values; ++i) {
      if (has_levels && def_levels[static_cast<size_t>(i)] < static_cast<uint32_t>(max_def_level)) {
        pq_append_null_cell(state);
      } else {
        Cell cell;
        if (!iterator.next(cell)) {
          err = iterator.error();
          return -1;
        }
        state.values.push_back(std::move(cell));
      }
    }
  } else if (encoding == PQ_ENC_PLAIN_DICTIONARY || encoding == PQ_ENC_RLE_DICTIONARY) {
    if (!state.has_dictionary) {
      err = "parquet: data page uses a dictionary but no dictionary page was found";
      return -1;
    }
    if (values_len < 1) {
      err = "parquet: truncated dictionary index stream";
      return -1;
    }
    const int32_t width = values_data[0];
    if (width < 0 || width > 32) {
      err = "parquet: invalid dictionary index bit width";
      return -1;
    }
    PqValueIterator iterator;
    iterator.init_dict(leaf, state.dictionary, values_data + 1, values_len - 1, width);
    for (int64_t i = 0; i < num_values; ++i) {
      if (has_levels && def_levels[static_cast<size_t>(i)] < static_cast<uint32_t>(max_def_level)) {
        pq_append_null_cell(state);
      } else {
        Cell cell;
        if (!iterator.next(cell)) {
          err = iterator.error();
          return -1;
        }
        state.values.push_back(std::move(cell));
      }
    }
  } else if (encoding == PQ_ENC_RLE && leaf.physical_type == PQ_PHYS_BOOLEAN) {
    if (values_len < 4) {
      err = "parquet: truncated boolean RLE stream";
      return -1;
    }
    const int64_t len = static_cast<int64_t>(pq_load_u32(values_data));
    if (len < 0 || len > values_len - 4) {
      err = "parquet: invalid boolean RLE stream length";
      return -1;
    }
    PqRleBitPackedDecoder decoder;
    decoder.init(values_data + 4, len, 1);
    for (int64_t i = 0; i < num_values; ++i) {
      if (has_levels && def_levels[static_cast<size_t>(i)] < static_cast<uint32_t>(max_def_level)) {
        pq_append_null_cell(state);
      } else {
        uint32_t value = 0;
        if (!decoder.next(value)) {
          err = "parquet: malformed boolean RLE stream";
          return -1;
        }
        Cell cell;
        pq_reset_cell(cell, FILE_COL_BOOLEAN);
        cell.bool_value = value != 0;
        state.values.push_back(std::move(cell));
      }
    }
  } else {
    err = "parquet: unsupported encoding " + std::to_string(encoding);
    return -1;
  }
  state.values_seen += num_values;
  return 0;
}

// ---------------------------------------------------------------------------
// Streaming reader.
// ---------------------------------------------------------------------------
int pq_load_footer(PqFileReader &file,
                   const std::string &path,
                   PqFileMeta &meta,
                   std::string &err)
{
  const int64_t file_size = file.size();
  if (file_size < 12) {
    err = "parquet: file '" + path + "' is too small to be a parquet file";
    return -1;
  }
  std::vector<uint8_t> buffer;
  if (file.read_at(0, 4, buffer, err) != 0) {
    return -1;
  }
  if (std::memcmp(buffer.data(), "PAR1", 4) != 0) {
    err = "parquet: file '" + path + "' does not start with PAR1";
    return -1;
  }
  if (file.read_at(file_size - 8, 8, buffer, err) != 0) {
    return -1;
  }
  if (std::memcmp(buffer.data() + 4, "PAR1", 4) != 0) {
    err = "parquet: file '" + path + "' does not end with PAR1";
    return -1;
  }
  const int64_t footer_len = static_cast<int64_t>(pq_load_u32(buffer.data()));
  if (footer_len <= 0 || footer_len > file_size - 12) {
    err = "parquet: invalid footer length in file '" + path + "'";
    return -1;
  }
  if (footer_len > PQ_MAX_ALLOC_BYTES) {
    err = "parquet: footer is too large";
    return -1;
  }
  std::vector<uint8_t> footer;
  if (file.read_at(file_size - 8 - footer_len, footer_len, footer, err) != 0) {
    return -1;
  }
  return pq_parse_file_meta_data(footer.data(), footer_len, meta, err) ? 0 : -1;
}

int pq_build_file_columns(const PqFileMeta &meta,
                          std::vector<FileColumn> &columns,
                          std::string &err)
{
  columns.clear();
  for (size_t i = 0; i < meta.leaves.size(); ++i) {
    FileColumn column;
    column.name = meta.leaves[i].name;
    if (column.name.empty()) {
      column.name = "column" + std::to_string(i);
    }
    column.type = meta.leaves[i].file_type;
    column.nullable = meta.leaves[i].repetition != PQ_REP_REQUIRED;
    columns.push_back(std::move(column));
  }
  if (columns.empty()) {
    err = "parquet: schema has no columns";
    return -1;
  }
  return 0;
}

class PqBatchReader : public FileBatchReader
{
public:
  PqBatchReader() {}
  ~PqBatchReader() override { close_file(); }

  int open(const std::string &path,
           const FileScanOptions &options,
           std::string &err) override
  {
    close_file();
    path_ = path;
    options_ = options;
    FileIdentity before;
    if (read_file_identity(path, before) != 0) {
      err = "parquet: cannot read the file identity: " + path;
      return FAILED;
    }
    if (file_.open(path, err) != 0) {
      return FAILED;
    }
    if (pq_load_footer(file_, path, meta_, err) != 0) {
      close_file();
      return FAILED;
    }
    if (build_projection(err) != 0) {
      close_file();
      return FAILED;
    }
    FileIdentity identity;
    if (read_file_identity(path, identity) != 0) {
      err = "parquet: cannot read the identity of file '" + path + "'";
      close_file();
      return FAILED;
    }
    if (identity != before) {
      err = "parquet: file changed while reading its schema: " + path;
      close_file();
      return FAILED;
    }
    identity_ = identity;
    row_group_index_ = 0;
    row_group_ready_ = false;
    row_group_rows_left_ = 0;
    return SUCCESS;
  }

  const std::vector<FileColumn> &columns() const override { return columns_; }

  FileIdentity identity() const override { return identity_; }

  int read_batch(int64_t max_rows,
                 std::vector<std::vector<Cell>> &rows,
                 std::string &err) override
  {
    err.clear();
    if (!check_identity(err)) {
      return FAILED;
    }
    if (max_rows <= 0) {
      return SUCCESS;
    }
    const size_t start_size = rows.size();
    int64_t emitted = 0;
    for (;;) {
      if (row_group_index_ >= meta_.row_groups.size()) {
        break;
      }
      if (!row_group_ready_) {
        if (init_row_group(err) != 0) {
          return FAILED;
        }
        row_group_ready_ = true;
      }
      while (emitted < max_rows && row_group_rows_left_ > 0) {
        const int64_t want = std::min(max_rows - emitted, PQ_MAX_BATCH_ROWS);
        for (size_t i = 0; i < states_.size(); ++i) {
          while (states_[i].available() < want && !states_[i].done) {
            const int rc = read_column_page(states_[i], err);
            if (rc < 0) {
              return FAILED;
            }
            if (rc == 0) {
              break;
            }
          }
        }
        int64_t available = std::numeric_limits<int64_t>::max();
        for (size_t i = 0; i < states_.size(); ++i) {
          available = std::min(available, states_[i].available());
        }
        if (available <= 0) {
          break;
        }
        int64_t count = std::min(available, want);
        count = std::min(count, row_group_rows_left_);
        for (int64_t r = 0; r < count; ++r) {
          std::vector<Cell> row;
          row.reserve(states_.size());
          for (size_t c = 0; c < states_.size(); ++c) {
            PqColumnState &state = states_[c];
            row.push_back(state.values[state.head + static_cast<size_t>(r)]);
          }
          rows.push_back(std::move(row));
        }
        for (size_t c = 0; c < states_.size(); ++c) {
          states_[c].consume(count);
        }
        row_group_rows_left_ -= count;
        emitted += count;
      }
      if (emitted >= max_rows) {
        break;
      }
      if (row_group_rows_left_ > 0) {
        err = "parquet: row group is truncated (expected " +
              std::to_string(meta_.row_groups[row_group_index_].num_rows) + " rows)";
        return FAILED;
      }
      ++row_group_index_;
      row_group_ready_ = false;
      states_.clear();
    }
    if (!check_identity(err)) {
      return FAILED;
    }
    if (rows.size() == start_size) {
      return EOF_REACHED;
    }
    return SUCCESS;
  }

private:
  bool check_identity(std::string &err) const
  {
    FileIdentity current;
    if (read_file_identity(path_, current) != 0 || current != identity_) {
      err = "parquet: file changed during scan: " + path_;
      return false;
    }
    return true;
  }

  void close_file()
  {
    file_.close();
    meta_ = PqFileMeta();
    columns_.clear();
    projection_.clear();
    states_.clear();
    identity_ = FileIdentity();
    row_group_index_ = 0;
    row_group_ready_ = false;
    row_group_rows_left_ = 0;
  }

  int build_projection(std::string &err)
  {
    projection_.clear();
    const int64_t leaf_count = static_cast<int64_t>(meta_.leaves.size());
    if (options_.projection.empty()) {
      for (int64_t i = 0; i < leaf_count; ++i) {
        projection_.push_back(static_cast<int32_t>(i));
      }
    } else {
      for (size_t i = 0; i < options_.projection.size(); ++i) {
        const int32_t index = options_.projection[i];
        if (index < 0 || index >= leaf_count) {
          err = "parquet: projection column index " + std::to_string(index) +
                " is out of range";
          return -1;
        }
        projection_.push_back(index);
      }
    }
    columns_.clear();
    for (size_t i = 0; i < projection_.size(); ++i) {
      const PqLeafSchema &leaf = meta_.leaves[static_cast<size_t>(projection_[i])];
      FileColumn column;
      column.name = leaf.name;
      if (column.name.empty()) {
        column.name = "column" + std::to_string(projection_[i]);
      }
      column.type = leaf.file_type;
      column.nullable = leaf.repetition != PQ_REP_REQUIRED;
      columns_.push_back(std::move(column));
    }
    // Reject unsupported codecs before the first batch is requested.
    for (size_t rg = 0; rg < meta_.row_groups.size(); ++rg) {
      const PqRowGroupMeta &row_group = meta_.row_groups[rg];
      for (size_t i = 0; i < projection_.size(); ++i) {
        const PqColumnChunkMeta &chunk =
            row_group.columns[static_cast<size_t>(projection_[i])];
        if (chunk.codec != PQ_CODEC_UNCOMPRESSED && chunk.codec != PQ_CODEC_SNAPPY) {
          err = "parquet: unsupported compression codec " + std::to_string(chunk.codec);
          return -1;
        }
      }
    }
    return 0;
  }

  int init_row_group(std::string &err)
  {
    const PqRowGroupMeta &row_group = meta_.row_groups[row_group_index_];
    states_.clear();
    states_.resize(projection_.size());
    for (size_t i = 0; i < projection_.size(); ++i) {
      const int32_t leaf_index = projection_[i];
      PqColumnState &state = states_[i];
      state.leaf_index = leaf_index;
      state.leaf = &meta_.leaves[static_cast<size_t>(leaf_index)];
      state.chunk = &row_group.columns[static_cast<size_t>(leaf_index)];
      state.file_type = state.leaf->file_type;
      state.max_def_level = state.leaf->repetition == PQ_REP_OPTIONAL ? 1 : 0;
      state.chunk_num_values = state.chunk->num_values;
      state.values_seen = 0;
      state.done = false;
      state.has_dictionary = false;
      state.dictionary.clear();
      state.values.clear();
      state.head = 0;

      // The chunk starts at its earliest valid page offset.  Some writers emit a
      // dictionary_page_offset or data_page_offset of 0 to mean "unknown" (and
      // empty chunks often only carry one of the two), so offsets below the
      // leading magic are ignored.  file_offset is only a last resort because
      // older writers store the metadata offset there.
      int64_t start = -1;
      if (state.chunk->dictionary_page_offset >= 4) {
        start = state.chunk->dictionary_page_offset;
      }
      if (state.chunk->data_page_offset >= 4 &&
          (start < 0 || state.chunk->data_page_offset < start)) {
        start = state.chunk->data_page_offset;
      }
      if (start < 0 && state.chunk->file_offset >= 4) {
        start = state.chunk->file_offset;
      }
      if (start < 4 || start >= file_.size()) {
        err = "parquet: column chunk offset is out of range";
        return -1;
      }
      int64_t end = 0;
      if (state.chunk->total_compressed_size > 0) {
        if (!pq_checked_add(start, state.chunk->total_compressed_size, end)) {
          err = "parquet: column chunk size is out of range";
          return -1;
        }
      } else {
        end = file_.size() - 8;
      }
      const int64_t maximum_end = file_.size() - 8;
      if (end > maximum_end) {
        end = maximum_end;
      }
      if (end < start) {
        err = "parquet: column chunk size is out of range";
        return -1;
      }
      state.next_offset = start;
      state.end_offset = end;
      if (state.chunk_num_values <= 0) {
        state.done = true;
      }
      if (state.next_offset >= state.end_offset) {
        state.done = true;
      }
    }
    row_group_rows_left_ = row_group.num_rows;
    return 0;
  }

  int read_column_page(PqColumnState &state, std::string &err)
  {
    for (;;) {
      if (state.values_seen >= state.chunk_num_values) {
        state.done = true;
        return 0;
      }
      if (state.next_offset >= state.end_offset) {
        state.done = true;
        return 0;
      }
      PqPageHeader header;
      int64_t header_len = 0;
      if (pq_read_page_header(file_, state.next_offset, header, header_len, err) != 0) {
        return -1;
      }
      int64_t body_offset = 0;
      if (!pq_checked_add(state.next_offset, header_len, body_offset)) {
        err = "parquet: page offset is out of range";
        return -1;
      }
      if (header.compressed_page_size < 0 ||
          header.compressed_page_size > PQ_MAX_ALLOC_BYTES) {
        err = "parquet: page size is out of range";
        return -1;
      }
      if (body_offset > file_.size() ||
          header.compressed_page_size > file_.size() - body_offset) {
        err = "parquet: page extends past the end of the file";
        return -1;
      }
      std::vector<uint8_t> body;
      if (file_.read_at(body_offset, header.compressed_page_size, body, err) != 0) {
        return -1;
      }
      if (!pq_checked_add(body_offset, header.compressed_page_size, state.next_offset)) {
        err = "parquet: page offset is out of range";
        return -1;
      }
      const int64_t before = state.available();
      switch (header.type) {
        case PQ_PAGE_DICTIONARY:
          if (handle_dictionary_page(state, header, body, err) != 0) {
            return -1;
          }
          continue;
        case PQ_PAGE_DATA:
          if (handle_data_page_v1(state, header, body, err) != 0) {
            return -1;
          }
          break;
        case PQ_PAGE_DATA_V2:
          if (handle_data_page_v2(state, header, body, err) != 0) {
            return -1;
          }
          break;
        case PQ_PAGE_INDEX:
          continue;
        default:
          err = "parquet: unsupported page type " + std::to_string(header.type);
          return -1;
      }
      if (state.available() > before) {
        return 1;
      }
    }
  }

  int handle_dictionary_page(PqColumnState &state,
                             const PqPageHeader &header,
                             const std::vector<uint8_t> &body,
                             std::string &err)
  {
    if (!header.has_dict_header) {
      err = "parquet: dictionary page has no dictionary page header";
      return -1;
    }
    if (header.dict_encoding >= 0 &&
        header.dict_encoding != PQ_ENC_PLAIN &&
        header.dict_encoding != PQ_ENC_PLAIN_DICTIONARY) {
      err = "parquet: unsupported encoding " + std::to_string(header.dict_encoding);
      return -1;
    }
    if (header.dict_num_values < 0 || header.dict_num_values > PQ_MAX_DICT_VALUES) {
      err = "parquet: dictionary is too large";
      return -1;
    }
    std::vector<uint8_t> decompressed;
    const int rc = pq_decompress_page(state.chunk->codec,
                                      body.data(),
                                      static_cast<int64_t>(body.size()),
                                      header.uncompressed_page_size,
                                      decompressed,
                                      err);
    if (rc != 0) {
      return -1;
    }
    PqValueIterator iterator;
    iterator.init_plain(*state.leaf, decompressed.data(), static_cast<int64_t>(decompressed.size()));
    state.dictionary.clear();
    for (int64_t i = 0; i < header.dict_num_values; ++i) {
      Cell cell;
      if (!iterator.next(cell)) {
        err = iterator.error();
        return -1;
      }
      state.dictionary.push_back(std::move(cell));
    }
    state.has_dictionary = true;
    return 0;
  }

  int handle_data_page_v1(PqColumnState &state,
                          const PqPageHeader &header,
                          const std::vector<uint8_t> &body,
                          std::string &err)
  {
    if (!header.has_data_header) {
      err = "parquet: data page has no data page header";
      return -1;
    }
    if (header.num_values < 0) {
      err = "parquet: data page has a negative value count";
      return -1;
    }
    if (header.num_values == 0) {
      return 0;
    }
    std::vector<uint8_t> decompressed;
    if (pq_decompress_page(state.chunk->codec,
                           body.data(),
                           static_cast<int64_t>(body.size()),
                           header.uncompressed_page_size,
                           decompressed,
                           err) != 0) {
      return -1;
    }
    int64_t pos = 0;
    std::vector<uint32_t> def_levels;
    if (pq_decode_levels_v1(decompressed.data(),
                            static_cast<int64_t>(decompressed.size()),
                            pos,
                            header.def_level_encoding,
                            header.num_values,
                            state.max_def_level,
                            def_levels,
                            err) != 0) {
      return -1;
    }
    return pq_decode_values(state,
                            header.encoding,
                            def_levels,
                            header.num_values,
                            decompressed.data() + pos,
                            static_cast<int64_t>(decompressed.size()) - pos,
                            err);
  }

  int handle_data_page_v2(PqColumnState &state,
                          const PqPageHeader &header,
                          const std::vector<uint8_t> &body,
                          std::string &err)
  {
    if (!header.has_v2_header) {
      err = "parquet: data page v2 has no data page header";
      return -1;
    }
    if (header.v2_num_values < 0 || header.v2_def_len < 0 || header.v2_rep_len < 0) {
      err = "parquet: data page v2 has invalid sizes";
      return -1;
    }
    if (header.v2_num_values == 0) {
      return 0;
    }
    const int64_t body_size = static_cast<int64_t>(body.size());
    const int64_t levels_len = static_cast<int64_t>(header.v2_rep_len) + header.v2_def_len;
    if (levels_len > body_size || levels_len > header.uncompressed_page_size) {
      err = "parquet: data page v2 levels are truncated";
      return -1;
    }
    const int64_t def_offset = header.v2_rep_len;
    const int64_t data_offset = levels_len;
    const int64_t data_size = body_size - data_offset;
    std::vector<uint32_t> def_levels;
    if (state.max_def_level > 0) {
      const int rc = pq_decode_levels_rle(body.data() + def_offset,
                                          header.v2_def_len,
                                          header.v2_num_values,
                                          state.max_def_level,
                                          def_levels,
                                          err);
      if (rc != 0) {
        return -1;
      }
    }
    std::vector<uint8_t> decompressed;
    const uint8_t *values_data = body.data() + data_offset;
    int64_t values_len = data_size;
    if (header.v2_is_compressed && state.chunk->codec != PQ_CODEC_UNCOMPRESSED) {
      const int64_t expected = static_cast<int64_t>(header.uncompressed_page_size) - levels_len;
      if (expected < 0) {
        err = "parquet: data page v2 has invalid sizes";
        return -1;
      }
      if (pq_decompress_page(state.chunk->codec,
                             values_data,
                             values_len,
                             expected,
                             decompressed,
                             err) != 0) {
        return -1;
      }
      values_data = decompressed.data();
      values_len = static_cast<int64_t>(decompressed.size());
    }
    return pq_decode_values(state,
                            header.v2_encoding,
                            def_levels,
                            header.v2_num_values,
                            values_data,
                            values_len,
                            err);
  }

  PqFileReader file_;
  std::string path_;
  FileScanOptions options_;
  PqFileMeta meta_;
  std::vector<FileColumn> columns_;
  std::vector<int32_t> projection_;
  std::vector<PqColumnState> states_;
  FileIdentity identity_;
  size_t row_group_index_ = 0;
  bool row_group_ready_ = false;
  int64_t row_group_rows_left_ = 0;
};

} // anonymous namespace

int infer_parquet_schema(const std::string &path,
                         const FileScanOptions &options,
                         std::vector<FileColumn> &columns,
                         FileIdentity &identity,
                         std::string &err)
{
  (void)options;
  err.clear();
  columns.clear();
  identity = FileIdentity();
  FileIdentity before;
  if (read_file_identity(path, before) != 0) {
    err = "parquet: cannot read the file identity: " + path;
    return -1;
  }
  PqFileReader file;
  if (file.open(path, err) != 0) {
    return -1;
  }
  PqFileMeta meta;
  if (pq_load_footer(file, path, meta, err) != 0) {
    return -1;
  }
  if (pq_build_file_columns(meta, columns, err) != 0) {
    return -1;
  }
  FileIdentity file_identity;
  if (read_file_identity(path, file_identity) != 0) {
    err = "parquet: cannot read the identity of file '" + path + "'";
    return -1;
  }
  if (file_identity != before) {
    err = "parquet: file changed during schema inference: " + path;
    return -1;
  }
  identity = file_identity;
  return 0;
}

FileBatchReader *create_parquet_reader()
{
  return new (std::nothrow) PqBatchReader();
}

} // namespace file_sql
} // namespace sql
} // namespace oceanbase
