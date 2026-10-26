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

#pragma once

#include <string>
#include <vector>

#include "common/object/ob_object.h"
#include "lib/string/ob_string.h"
#include "sql/engine/basic/ob_file_format.h"

namespace oceanbase
{
namespace sql
{

// Bridges the SQL layer (system variables, secure_file_priv, column types) and
// the pure C++ file readers in ob_file_format.cpp / ob_parquet_format.cpp.
class ObFileScanUtils
{
public:
  // Translates the SQL level options of a read_* table function into reader options.
  static int build_scan_options(int64_t format,
                                int64_t delimiter,
                                int64_t quote,
                                int64_t escape,
                                bool header,
                                const common::ObString &null_str,
                                const common::ObString &json_format,
                                int64_t sample_size,
                                file_sql::FileScanOptions &options,
                                std::string &err);
  // Infers the schema of a file.  Returns OB_SUCCESS and fills columns/identity.
  static int infer_schema(const common::ObString &path,
                          int64_t format,
                          const file_sql::FileScanOptions &options,
                          std::vector<file_sql::FileColumn> &columns,
                          file_sql::FileIdentity &identity,
                          int64_t &row_count,
                          std::string &err);
  // Expands a glob pattern into the matched regular files (sorted).
  static int glob_files(const common::ObString &pattern,
                        std::vector<std::string> &files,
                        std::string &err);
  // Maps an inferred column type to the SQL type, accuracy and collation used by
  // the resolver.
  static int column_type_to_sql_type(file_sql::FileColumnType type,
                                     int64_t max_length,
                                     common::ObObjType &obj_type,
                                     common::ObAccuracy &accuracy,
                                     common::ObCollationType &cs_type);
  static bool is_valid_delimiter(int64_t delimiter);
};

} // namespace sql
} // namespace oceanbase
