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
  if (pattern.empty()) { err = "glob pattern is empty"; return OB_INVALID_ARGUMENT; }
  const std::string path(pattern.ptr(), pattern.length());
  if (file_sql::discover_files(path, files, err) != 0) {
    return OB_IO_ERROR;
  }
  return OB_SUCCESS;
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
