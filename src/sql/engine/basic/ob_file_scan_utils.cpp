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

#define USING_LOG_PREFIX SQL_ENG

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <string>
#include <vector>

#include "sql/engine/basic/ob_file_scan_utils.h"
#include "sql/engine/basic/ob_parquet_format.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

namespace
{

bool match_glob_component(const std::string &pattern, const std::string &name)
{
  size_t p = 0;
  size_t n = 0;
  size_t star_p = std::string::npos;
  size_t star_n = 0;
  while (n < name.size()) {
    if (p < pattern.size() && (pattern[p] == '?' || pattern[p] == name[n])) {
      ++p;
      ++n;
    } else if (p < pattern.size() && pattern[p] == '*') {
      star_p = p++;
      star_n = n;
    } else if (star_p != std::string::npos) {
      p = star_p + 1;
      n = ++star_n;
    } else {
      return false;
    }
  }
  while (p < pattern.size() && pattern[p] == '*') {
    ++p;
  }
  return p == pattern.size();
}

bool has_wildcard(const std::string &s)
{
  return s.find_first_of("*?") != std::string::npos;
}

int split_path(const std::string &path, std::vector<std::string> &parts, bool &absolute)
{
  int ret = OB_SUCCESS;
  std::string cur;
  absolute = !path.empty() && (path[0] == '/' || (path.size() > 1 && path[1] == ':'));
  const size_t start = (path.size() > 1 && path[1] == ':') ? 2 : 0;
  for (size_t i = start; OB_SUCC(ret) && i < path.size(); ++i) {
    const char c = path[i];
    if (c == '/' || c == '\\') {
      if (!cur.empty()) {
        parts.push_back(cur);
        cur.clear();
      }
    } else {
      cur.push_back(c);
    }
  }
  if (!cur.empty()) {
    parts.push_back(cur);
  }
  if (parts.empty()) {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int glob_walk(const std::string &prefix,
              const std::vector<std::string> &parts,
              size_t idx,
              std::vector<std::string> &out,
              std::string &err)
{
  int ret = OB_SUCCESS;
  namespace fs = std::filesystem;
  if (idx >= parts.size()) {
    std::error_code ec;
    if (fs::is_regular_file(fs::path(prefix), ec) && !ec) {
      out.push_back(prefix);
    }
    return ret;
  }
  const std::string &part = parts[idx];
  const bool last = (idx + 1 == parts.size());
  if (!has_wildcard(part)) {
    std::string next = prefix;
    if (!next.empty() && next.back() != '/' && next.back() != '\\') {
      next.push_back('/');
    }
    next += part;
    ret = glob_walk(next, parts, idx + 1, out, err);
  } else {
    std::string dir = prefix.empty() ? std::string(".") : prefix;
    std::error_code ec;
    fs::directory_iterator it(dir, ec);
    if (ec) {
      err.assign("cannot open directory: " + dir);
      ret = OB_IO_ERROR;
    } else {
      for (fs::directory_iterator end; OB_SUCC(ret) && it != end; it.increment(ec)) {
        if (ec) {
          err.assign("cannot read directory: " + dir);
          ret = OB_IO_ERROR;
          break;
        }
        const std::string name = it->path().filename().string();
        if (name.empty() || name[0] == '.') {
          continue;
        }
        std::error_code tec;
        const bool is_dir = it->is_directory(tec);
        if (!last && (!is_dir || tec)) {
          continue;
        }
        if (match_glob_component(part, name)) {
          ret = glob_walk(it->path().string(), parts, idx + 1, out, err);
        }
      }
    }
  }
  return ret;
}

} // namespace

bool ObFileScanUtils::is_valid_delimiter(int64_t delimiter)
{
  return delimiter > 0 && delimiter <= 0xff;
}

int ObFileScanUtils::build_scan_options(int64_t format,
                                        int64_t delimiter,
                                        int64_t quote,
                                        int64_t escape,
                                        bool header,
                                        const ObString &null_str,
                                        const ObString &json_format,
                                        int64_t sample_size,
                                        file_sql::FileScanOptions &options,
                                        std::string &err)
{
  int ret = OB_SUCCESS;
  options = file_sql::FileScanOptions();
  if (format == 0) {
    options.format = file_sql::FILE_FORMAT_CSV;
  } else if (format == 1 || format == 2) {
    options.format = file_sql::FILE_FORMAT_JSON;
  } else if (format == 3) {
    options.format = file_sql::FILE_FORMAT_PARQUET;
  } else {
    err.assign("glob() is not a file reader");
    ret = OB_NOT_SUPPORTED;
  }
  if (OB_SUCC(ret)) {
    if (!is_valid_delimiter(delimiter)) {
      err.assign("invalid delimiter, it must be a single byte");
      ret = OB_INVALID_ARGUMENT;
    } else if (quote < 0 || quote > 0xff || escape < 0 || escape > 0xff) {
      err.assign("invalid quote or escape character");
      ret = OB_INVALID_ARGUMENT;
    } else {
      options.delimiter = static_cast<char>(delimiter);
      options.quote = static_cast<char>(quote);
      options.escape = static_cast<char>(escape);
      options.header = header;
      options.sample_size = sample_size;
      if (!null_str.empty()) {
        options.null_str.assign(null_str.ptr(), null_str.length());
      }
      if (!json_format.empty()) {
        options.json_format.assign(json_format.ptr(), json_format.length());
      }
    }
  }
  return ret;
}

int ObFileScanUtils::infer_schema(const ObString &path,
                                  int64_t format,
                                  const file_sql::FileScanOptions &options,
                                  std::vector<file_sql::FileColumn> &columns,
                                  file_sql::FileIdentity &identity,
                                  int64_t &row_count,
                                  std::string &err)
{
  int ret = OB_SUCCESS;
  const std::string file(path.ptr(), path.length());
  row_count = 0;
  if (path.empty()) {
    err.assign("file path is empty");
    ret = OB_INVALID_ARGUMENT;
  } else {
    int rc = 0;
    if (format == 0) {
      rc = file_sql::infer_csv_schema(file, options, columns, identity, err);
    } else if (format == 1 || format == 2) {
      rc = file_sql::infer_json_schema(file, options, columns, identity, err);
    } else if (format == 3) {
      rc = file_sql::infer_parquet_schema(file, options, columns, identity, err);
    } else {
      err.assign("unsupported file format");
      ret = OB_NOT_SUPPORTED;
    }
    if (OB_SUCC(ret) && 0 != rc) {
      // the readers report a failure with an explanatory message
      ret = OB_IO_ERROR;
      LOG_WARN("failed to infer the schema of the file", K(ret), K(err.c_str()));
    }
  }
  if (OB_SUCC(ret)) {
    if (columns.empty()) {
      err.assign("no column can be inferred from the file");
      ret = OB_ERR_UNEXPECTED;
    } else {
      // A rough cardinality for the optimizer.  Row level readers do not count
      // rows during inference to keep the pass cheap.
      const int64_t bytes_per_row = 24;
      row_count = identity.size > 0 ? std::max<int64_t>(1, identity.size / bytes_per_row) : 1000;
      row_count = std::min<int64_t>(row_count, 10000000);
    }
  }
  return ret;
}

int ObFileScanUtils::glob_files(const ObString &pattern,
                                std::vector<std::string> &files,
                                std::string &err)
{
  int ret = OB_SUCCESS;
  namespace fs = std::filesystem;
  std::vector<std::string> parts;
  bool absolute = false;
  const std::string pat(pattern.ptr(), pattern.length());
  if (pat.empty()) {
    err.assign("glob pattern is empty");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(split_path(pat, parts, absolute))) {
    err.assign("invalid glob pattern: " + pat);
  } else if (!has_wildcard(pat)) {
    std::error_code ec;
    if (fs::is_regular_file(fs::path(pat), ec) && !ec) {
      files.push_back(pat);
    } else {
      err.assign("file does not exist: " + pat);
      ret = OB_FILE_NOT_EXIST;
    }
  } else {
    std::string prefix;
    if (absolute) {
      if (pat.size() > 1 && pat[1] == ':') {
        prefix.assign(pat.begin(), pat.begin() + 2);
      } else {
        prefix = "/";
      }
    }
    ret = glob_walk(prefix, parts, 0, files, err);
    if (OB_SUCC(ret)) {
      for (size_t i = 0; i < files.size(); ++i) {
        for (size_t j = 0; j < files.at(i).size(); ++j) {
          if ('\\' == files.at(i)[j]) {
            files.at(i)[j] = '/';
          }
        }
      }
      std::sort(files.begin(), files.end());
    }
  }
  return ret;
}

int ObFileScanUtils::column_type_to_sql_type(file_sql::FileColumnType type,
                                             int64_t max_length,
                                             ObObjType &obj_type,
                                             ObAccuracy &accuracy,
                                             ObCollationType &cs_type)
{
  int ret = OB_SUCCESS;
  cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  switch (type) {
    case file_sql::FILE_COL_BIGINT:
      obj_type = ObIntType;
      accuracy.set_length(20);
      break;
    case file_sql::FILE_COL_DOUBLE:
      obj_type = ObDoubleType;
      accuracy.set_length(22);
      break;
    case file_sql::FILE_COL_BOOLEAN:
      obj_type = ObTinyIntType;
      accuracy.set_length(1);
      break;
    case file_sql::FILE_COL_DATE:
      obj_type = ObDateType;
      accuracy.set_length(10);
      break;
    case file_sql::FILE_COL_DATETIME:
      obj_type = ObDateTimeType;
      accuracy.set_length(26);
      accuracy.set_precision(0);
      break;
    case file_sql::FILE_COL_VARCHAR:
    case file_sql::FILE_COL_NULL:
    default:
      obj_type = ObVarcharType;
      accuracy.set_length(max_length > 0 ? max_length : OB_MAX_VARCHAR_LENGTH);
      break;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
