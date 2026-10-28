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

#include <vector>

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_file_format.h"

namespace oceanbase
{
namespace sql
{

// Physical spec of read_csv()/read_json()/read_parquet()/glob().  The operator is
// a leaf: it produces the columns of the table item, no child input.
class ObFileTableSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObFileTableSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      column_exprs_(alloc),
      column_indexes_(alloc),
      column_types_(alloc),
      path_(),
      format_(0),
      delimiter_(','),
      quote_('"'),
      escape_('"'),
      has_header_(true),
      null_str_(),
      json_format_(),
      sample_size_(-1),
      file_size_(0),
      file_mtime_ns_(0), file_device_(0), file_inode_(0), file_ctime_ns_(0), file_native_identity_(false),
      is_static_(false),
      static_column_count_(0),
      static_cells_(alloc)
  {}
  // Output expressions, one entry per projected column.
  common::ObFixedArray<ObExpr*, common::ObIAllocator> column_exprs_;
  // For every entry of column_exprs_, the 0 based index of the source column.
  common::ObFixedArray<int64_t, common::ObIAllocator> column_indexes_;
  // The inferred type (file_sql::FileColumnType) of every projected column.
  common::ObFixedArray<int64_t, common::ObIAllocator> column_types_;
  common::ObString path_;
  int64_t format_;
  int64_t delimiter_;
  int64_t quote_;
  int64_t escape_;
  bool has_header_;
  common::ObString null_str_;
  common::ObString json_format_;
  int64_t sample_size_;
  int64_t file_size_;
  int64_t file_mtime_ns_;
  uint64_t file_device_;
  uint64_t file_inode_;
  int64_t file_ctime_ns_;
  bool file_native_identity_;
  // A static table (glob()/describe output) carries its rows in the spec.
  bool is_static_;
  int64_t static_column_count_;
  common::ObFixedArray<common::ObString, common::ObIAllocator> static_cells_;
};

class ObFileTableOp : public ObOperator
{
public:
  ObFileTableOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
      reader_(nullptr),
      static_pos_(0),
      batch_(),
      batch_pos_(0),
      eof_(false),
      row_count_(0)
  {}
  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override;

private:
  int init_reader();
  int fetch_batch();
  int make_scan_options(file_sql::FileScanOptions &options);
  int fill_scan_row(const std::vector<file_sql::Cell> &row);
  int fill_static_row(const int64_t row_idx);
  int set_cell_datum(ObExpr *expr, const file_sql::Cell &cell, const int64_t column_type);

private:
  file_sql::FileBatchReader *reader_;
  int64_t static_pos_;
  std::vector<std::vector<file_sql::Cell>> batch_;
  int64_t batch_pos_;
  bool eof_;
  int64_t row_count_;
  // Sorted, unique source column indexes handed to the reader as the projection.
  std::vector<int32_t> projection_;
  // Position of every output column inside a (projected) reader row.
  std::vector<int64_t> row_positions_;
};

} // namespace sql
} // namespace oceanbase
