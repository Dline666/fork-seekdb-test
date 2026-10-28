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
#include <string>
#include <vector>

#include "sql/engine/basic/ob_file_table_op.h"
#include "sql/engine/basic/ob_parquet_format.h"
#include "sql/engine/ob_exec_context.h"
#include "common/timezone/ob_time_convert.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

namespace
{

// The SQL level function codes are read_csv=0, read_json=1, read_ndjson=2,
// read_parquet=3 and glob=4; the readers use file_sql::FileFormat.
int32_t ob_file_table_reader_format(const int64_t sql_format)
{
  int32_t format = file_sql::FILE_FORMAT_CSV;
  if (0 == sql_format) {
    format = file_sql::FILE_FORMAT_CSV;
  } else if (1 == sql_format || 2 == sql_format) {
    format = file_sql::FILE_FORMAT_JSON;
  } else if (3 == sql_format) {
    format = file_sql::FILE_FORMAT_PARQUET;
  }
  return format;
}

} // namespace

OB_SERIALIZE_MEMBER((ObFileTableSpec, ObOpSpec),
                    column_exprs_,
                    column_indexes_,
                    column_types_,
                    path_,
                    format_,
                    delimiter_,
                    quote_,
                    escape_,
                    has_header_,
                    null_str_,
                    json_format_,
                    sample_size_,
                    file_size_,
                    file_mtime_ns_,
                    is_static_,
                    static_column_count_,
                    static_cells_);

int ObFileTableOp::inner_open()
{
  int ret = OB_SUCCESS;
  batch_.clear();
  batch_pos_ = 0;
  static_pos_ = 0;
  eof_ = false;
  row_count_ = 0;
  if (OB_FAIL(ObOperator::inner_open())) {
  } else if (MY_SPEC.is_static_) {
    if (OB_UNLIKELY(MY_SPEC.static_column_count_ <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid static file table", K(ret), K(MY_SPEC.static_column_count_));
    } else {
      row_count_ = MY_SPEC.static_cells_.count() / MY_SPEC.static_column_count_;
    }
  } else if (OB_FAIL(init_reader())) {
  }
  return ret;
}

int ObFileTableOp::init_reader()
{
  int ret = OB_SUCCESS;
  file_sql::FileScanOptions options;
  file_sql::FileIdentity identity;
  const std::string path(MY_SPEC.path_.ptr(), MY_SPEC.path_.length());
  if (OB_FAIL(make_scan_options(options))) {
  } else if (0 != file_sql::read_file_identity(path, identity)) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("failed to read file identity", K(ret), K(path.c_str()));
  } else if (!(identity == file_sql::FileIdentity{MY_SPEC.file_size_, MY_SPEC.file_mtime_ns_, true})) {
    ret = OB_IO_ERROR;
    LOG_WARN("the file was modified after the schema was inferred", K(ret),
             K(path.c_str()), K(identity.size), K(identity.mtime_ns),
             K(MY_SPEC.file_size_), K(MY_SPEC.file_mtime_ns_));
  } else if (OB_ISNULL(reader_ = file_sql::create_file_reader(
                ob_file_table_reader_format(MY_SPEC.format_)))) {
    ret = OB_NOT_SUPPORTED;
    char fmt_buf[64] = {0};
    snprintf(fmt_buf, sizeof(fmt_buf), "file format %ld", static_cast<long>(MY_SPEC.format_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, fmt_buf);
    LOG_ERROR("unsupported file format", K(ret), K(MY_SPEC.format_));
  } else {
    std::string err;
    if (0 != reader_->open(path, options, err)) {
      ret = OB_IO_ERROR;
      LOG_ERROR("failed to open file", K(ret), K(path.c_str()), K(err.c_str()));
    } else if (reader_->identity() != identity) {
      // open() may perform another schema pass. Do not use its rows with the
      // stale types and column indexes captured by the physical plan.
      ret = OB_IO_ERROR;
      LOG_WARN("the file changed while the reader was opening", K(ret), K(path.c_str()));
    }
  }
  return ret;
}

int ObFileTableOp::make_scan_options(file_sql::FileScanOptions &options)
{
  int ret = OB_SUCCESS;
  options = file_sql::FileScanOptions();
  options.format = ob_file_table_reader_format(MY_SPEC.format_);
  options.delimiter = static_cast<char>(MY_SPEC.delimiter_);
  options.quote = static_cast<char>(MY_SPEC.quote_);
  options.escape = static_cast<char>(MY_SPEC.escape_);
  options.header = MY_SPEC.has_header_;
  options.sample_size = MY_SPEC.sample_size_;
  if (!MY_SPEC.null_str_.empty()) {
    options.null_str.assign(MY_SPEC.null_str_.ptr(), MY_SPEC.null_str_.length());
  }
  if (!MY_SPEC.json_format_.empty()) {
    options.json_format.assign(MY_SPEC.json_format_.ptr(), MY_SPEC.json_format_.length());
  }
  // Column pruning: sorted unique source column indexes.  The reader returns
  // rows with exactly projection.size() cells in projection order, so remember
  // where every output column lives inside a row.
  projection_.clear();
  row_positions_.clear();
  for (int64_t i = 0; i < MY_SPEC.column_indexes_.count(); ++i) {
    projection_.push_back(static_cast<int32_t>(MY_SPEC.column_indexes_.at(i)));
  }
  std::sort(projection_.begin(), projection_.end());
  projection_.erase(std::unique(projection_.begin(), projection_.end()), projection_.end());
  for (int64_t i = 0; i < MY_SPEC.column_indexes_.count(); ++i) {
    const int32_t index = static_cast<int32_t>(MY_SPEC.column_indexes_.at(i));
    const std::vector<int32_t>::const_iterator it =
        std::lower_bound(projection_.begin(), projection_.end(), index);
    row_positions_.push_back(it == projection_.end() ? -1 : (it - projection_.begin()));
  }
  options.projection = projection_;
  return ret;
}

int ObFileTableOp::fetch_batch()
{
  int ret = OB_SUCCESS;
  const int64_t batch_size = 256;
  std::string err;
  batch_.clear();
  batch_pos_ = 0;
  if (OB_ISNULL(reader_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const int r = reader_->read_batch(batch_size, batch_, err);
    if (file_sql::FileBatchReader::EOF_REACHED == r) {
      eof_ = true;
      if (batch_.empty()) {
        ret = OB_ITER_END;
      }
    } else if (file_sql::FileBatchReader::SUCCESS != r) {
      ret = OB_IO_ERROR;
      LOG_ERROR("failed to read file batch", K(ret), K(err.c_str()));
    }
  }
  return ret;
}

int ObFileTableOp::set_cell_datum(ObExpr *expr, const file_sql::Cell &cell, const int64_t column_type)
{
  int ret = OB_SUCCESS;
  ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
  if (cell.is_null) {
    datum.set_null();
  } else if (file_sql::FILE_COL_BIGINT == column_type || file_sql::FILE_COL_BOOLEAN == column_type) {
    datum.set_int(file_sql::FILE_COL_BOOLEAN == column_type
                      ? static_cast<int64_t>(cell.bool_value ? 1 : 0)
                      : cell.int_value);
  } else if (file_sql::FILE_COL_DOUBLE == column_type) {
    datum.set_double(cell.double_value);
  } else if (file_sql::FILE_COL_DATE == column_type) {
    int32_t date_value = 0;
    const ObString str(static_cast<int32_t>(cell.str_value.size()), cell.str_value.data());
    if (OB_FAIL(ObTimeConverter::str_to_date(str, date_value))) {
      datum.set_null();
      ret = OB_SUCCESS;
    } else {
      datum.set_date(date_value);
    }
  } else if (file_sql::FILE_COL_DATETIME == column_type) {
    int64_t datetime_value = 0;
    ObTimeZoneInfo tz_info;
    ObBasicSessionInfo *session = ctx_.get_my_session();
    const ObTimeZoneInfo *tz = nullptr;
    if (nullptr != session) {
      tz = session->get_timezone_info();
    }
    if (nullptr == tz) {
      tz = &tz_info;
    }
    ObTimeConvertCtx cvrt_ctx(tz, false);
    const ObString str(static_cast<int32_t>(cell.str_value.size()), cell.str_value.data());
    if (OB_FAIL(ObTimeConverter::str_to_datetime(str, cvrt_ctx, datetime_value))) {
      datum.set_null();
      ret = OB_SUCCESS;
    } else {
      datum.set_datetime(datetime_value);
    }
  } else {
    datum.set_string(cell.str_value.data(), static_cast<uint32_t>(cell.str_value.size()));
  }
  if (OB_SUCC(ret)) {
    expr->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}

int ObFileTableOp::fill_scan_row(const std::vector<file_sql::Cell> &row)
{
  int ret = OB_SUCCESS;
  const int64_t expr_count = MY_SPEC.column_exprs_.count();
  CK(expr_count == MY_SPEC.column_indexes_.count());
  CK(expr_count == MY_SPEC.column_types_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < expr_count; ++i) {
    const int64_t pos = (i < static_cast<int64_t>(row_positions_.size()))
                            ? row_positions_.at(i) : -1;
    if (OB_UNLIKELY(pos < 0 || pos >= static_cast<int64_t>(row.size()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file row is shorter than the projected schema", K(ret), K(pos),
               K(row.size()));
    } else if (OB_FAIL(set_cell_datum(MY_SPEC.column_exprs_.at(i), row.at(pos),
                                      MY_SPEC.column_types_.at(i)))) {
    }
  }
  return ret;
}

int ObFileTableOp::fill_static_row(const int64_t row_idx)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = MY_SPEC.static_column_count_;
  const int64_t expr_count = MY_SPEC.column_exprs_.count();
  CK(col_count > 0);
  CK(expr_count == MY_SPEC.column_indexes_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < expr_count; ++i) {
    const int64_t src_index = MY_SPEC.column_indexes_.at(i);
    const int64_t cell_index = row_idx * col_count + src_index;
    if (OB_UNLIKELY(src_index < 0 || src_index >= col_count)
        || OB_UNLIKELY(cell_index < 0 || cell_index >= MY_SPEC.static_cells_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid static file table cell", K(ret), K(row_idx), K(src_index));
    } else {
      const ObString &str = MY_SPEC.static_cells_.at(cell_index);
      ObDatum &datum = MY_SPEC.column_exprs_.at(i)->locate_datum_for_write(eval_ctx_);
      datum.set_string(str);
      MY_SPEC.column_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
    }
  }
  return ret;
}

int ObFileTableOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(try_check_status())) {
  } else if (MY_SPEC.is_static_) {
    if (static_pos_ >= row_count_) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(fill_static_row(static_pos_))) {
    } else {
      ++static_pos_;
    }
  } else {
    if (batch_pos_ >= static_cast<int64_t>(batch_.size()) && !eof_) {
      ret = fetch_batch();
    }
    if (OB_ITER_END == ret) {
      // keep OB_ITER_END
    } else if (OB_FAIL(ret)) {
    } else if (batch_pos_ >= static_cast<int64_t>(batch_.size())) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(fill_scan_row(batch_.at(batch_pos_)))) {
    } else {
      ++batch_pos_;
    }
  }
  return ret;
}

int ObFileTableOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else {
    batch_.clear();
    batch_pos_ = 0;
    eof_ = false;
    static_pos_ = 0;
    if (nullptr != reader_) {
      delete reader_;
      reader_ = nullptr;
    }
    if (!MY_SPEC.is_static_ && OB_FAIL(init_reader())) {
      LOG_WARN("failed to reopen the file for rescan", K(ret));
    }
  }
  return ret;
}

int ObFileTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (nullptr != reader_) {
    delete reader_;
    reader_ = nullptr;
  }
  batch_.clear();
  batch_pos_ = 0;
  eof_ = false;
  static_pos_ = 0;
  return ret;
}

void ObFileTableOp::destroy()
{
  if (nullptr != reader_) {
    delete reader_;
    reader_ = nullptr;
  }
  batch_.clear();
  ObOperator::destroy();
}

} // namespace sql
} // namespace oceanbase
