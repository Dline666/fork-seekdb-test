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

#include "sql/engine/basic/ob_file_format.h"

namespace oceanbase
{
namespace sql
{
namespace file_sql
{

// Infers the schema of a parquet file from the footer metadata.
int infer_parquet_schema(const std::string &path,
                         const FileScanOptions &options,
                         std::vector<FileColumn> &columns,
                         FileIdentity &identity,
                         std::string &err);

// Creates a streaming parquet reader.  The caller owns the returned object.
FileBatchReader *create_parquet_reader();

} // namespace file_sql
} // namespace sql
} // namespace oceanbase
