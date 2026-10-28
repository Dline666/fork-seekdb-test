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

#include <cstdint>
#include <string>
#include <vector>

namespace oceanbase
{
namespace sql
{
namespace file_sql
{

// The file formats supported by the "file as database" table functions.
enum FileFormat : int32_t
{
  FILE_FORMAT_CSV = 0,
  FILE_FORMAT_JSON = 1,
  FILE_FORMAT_PARQUET = 2,
};

// Deterministic inference result types.  They are mapped to SQL types when the
// schema is handed to the resolver: BIGINT -> int64, DOUBLE -> double,
// BOOLEAN -> tinyint(1), VARCHAR -> utf8 varchar, DATE -> date,
// DATETIME -> datetime, NULL -> varchar (an all-null column has no value type).
enum FileColumnType : int32_t
{
  FILE_COL_BIGINT = 0,
  FILE_COL_DOUBLE = 1,
  FILE_COL_BOOLEAN = 2,
  FILE_COL_VARCHAR = 3,
  FILE_COL_DATE = 4,
  FILE_COL_DATETIME = 5,
  FILE_COL_NULL = 6,
};

const char *file_column_type_name(FileColumnType type);

struct FileColumn
{
  std::string name;
  FileColumnType type = FILE_COL_VARCHAR;
  bool nullable = true;
};

// Options accepted by read_csv()/read_json()/read_parquet().
struct FileScanOptions
{
  int32_t format = FILE_FORMAT_CSV;
  char delimiter = ',';
  char quote = '"';
  // '"' means the RFC4180 doubling rule, otherwise an explicit escape char.
  char escape = '"';
  bool header = true;
  // Text that represents NULL (only meaningful when non-empty).
  std::string null_str;
  int64_t sample_size = -1;
  // "auto" / "newline_delimited" / "array" / "object".
  std::string json_format;
  // Column indexes to read, empty means all columns.
  std::vector<int32_t> projection;
};

// One value of a row.  Values are kept in a decoded, SQL-friendly form.
struct Cell
{
  bool is_null = true;
  FileColumnType type = FILE_COL_NULL;
  int64_t int_value = 0;
  double double_value = 0;
  bool bool_value = false;
  // varchar / date ("YYYY-MM-DD") / datetime ("YYYY-MM-DD HH:MM:SS[.ffffff]")
  std::string str_value;
};

// Identity of a file, used to detect changes between schema inference and scan.
struct FileIdentity
{
  int64_t size = 0;
  int64_t mtime_ns = 0;
  bool valid = false;
  bool operator==(const FileIdentity &other) const
  {
    return valid && other.valid && size == other.size && mtime_ns == other.mtime_ns;
  }
  bool operator!=(const FileIdentity &other) const { return !(*this == other); }
};

// A streaming reader.  Implementations never materialize the whole file.
class FileBatchReader
{
public:
  static const int SUCCESS = 0;
  static const int EOF_REACHED = 1;
  static const int FAILED = -1;

  virtual ~FileBatchReader() {}
  // Opens the file and prepares the scan.  Returns SUCCESS or FAILED.
  virtual int open(const std::string &path,
                   const FileScanOptions &options,
                   std::string &err) = 0;
  virtual const std::vector<FileColumn> &columns() const = 0;
  // Appends at most max_rows rows to rows.  Returns SUCCESS, EOF_REACHED or FAILED.
  virtual int read_batch(int64_t max_rows,
                         std::vector<std::vector<Cell>> &rows,
                         std::string &err) = 0;
  virtual FileIdentity identity() const = 0;
};

// Canonicalizes an input (or the fixed directory prefix of a glob) and checks
// confinement to an existing secure directory. Rejects NULs, traversal after a
// wildcard and symlink escapes. No server-wide allowlist bypass is applied.
bool validate_file_path(const std::string &path,
                        const std::string &secure_directory,
                        bool glob,
                        std::string &resolved_path,
                        std::string &err);

// Detects the format from the file extension.  Returns false for unsupported
// extensions.
bool detect_format_by_extension(const std::string &path, int32_t &format);

// Infers the schema of a CSV or JSONL file by streaming it once.  Sample size is
// always the whole file for these formats (sample_size must be -1).
int infer_csv_schema(const std::string &path,
                     const FileScanOptions &options,
                     std::vector<FileColumn> &columns,
                     FileIdentity &identity,
                     std::string &err);

int infer_json_schema(const std::string &path,
                      const FileScanOptions &options,
                      std::vector<FileColumn> &columns,
                      FileIdentity &identity,
                      std::string &err);

// Infers the schema of a parquet file from its footer.
int infer_parquet_schema(const std::string &path,
                         const FileScanOptions &options,
                         std::vector<FileColumn> &columns,
                         FileIdentity &identity,
                         std::string &err);

// Factory: creates a reader for the format described by options.format.
FileBatchReader *create_file_reader(int32_t format);

// Reads the file identity (size + last modification time).
int read_file_identity(const std::string &path, FileIdentity &identity);

// CSV/JSON writers used by COPY ... TO.
struct CsvWriteOptions
{
  char delimiter = ',';
  char quote = '"';
  char escape = '"';
  bool header = true;
  std::string null_str;
  std::string line_terminator = "\n";
};

class FileBatchWriter
{
public:
  virtual ~FileBatchWriter() {}
  virtual int open(const std::string &path, std::string &err) = 0;
  // Writes one row; values are rendered from the cell description.
  virtual int write_row(const std::vector<Cell> &row, std::string &err) = 0;
  // Flushes buffers and moves the temporary file to its final name.
  virtual int close(std::string &err) = 0;
  // Discards everything written so far (used when the query fails).
  virtual void abort() = 0;
};

FileBatchWriter *create_csv_writer(const CsvWriteOptions &options);

} // namespace file_sql
} // namespace sql
} // namespace oceanbase
