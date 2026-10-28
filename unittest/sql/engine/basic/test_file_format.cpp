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

// Standalone regression tests for the standard-library-only file SQL core.
// Build with ob_file_format.cpp and ob_parquet_format.cpp, -std=c++17 -Isrc.
#include "sql/engine/basic/ob_file_format.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
using namespace oceanbase::sql::file_sql;
namespace fs = std::filesystem;
static fs::path root;
static fs::path fixture;
static void put(const fs::path &p, const std::string &s) { std::ofstream(p, std::ios::binary) << s; }
static std::string get(const fs::path &p) { std::ifstream f(p, std::ios::binary); std::ostringstream s; s << f.rdbuf(); return s.str(); }
static Cell text(const std::string &s) { Cell c; c.is_null=false; c.type=FILE_COL_VARCHAR; c.str_value=s; return c; }
#define CHECK(x) do { if (!(x)) { std::cerr << __func__ << ":" << __LINE__ << ": " #x " FAILED\n"; return false; } } while (0)
static bool existing_target() {
  std::string e; const auto p=root/"existing.csv"; put(p,"sentinel");
  std::unique_ptr<FileBatchWriter> w(create_csv_writer({}));
  CHECK(w->open(p.string(),e)!=0); CHECK(get(p)=="sentinel"); return true;
}
static bool preexisting_temporary_file() {
  std::string e; const auto p=root/"temp.csv"; put(p.string()+".tmp","sentinel");
  std::unique_ptr<FileBatchWriter> w(create_csv_writer({}));
  CHECK(w->open(p.string(),e)==0); CHECK(w->write_row({text("new")},e)==0); CHECK(w->close(e)==0);
  CHECK(get(p.string()+".tmp")=="sentinel"); CHECK(get(p)=="new\n"); return true;
}
static bool temporary_symlink() {
  std::string e; const auto p=root/"symlink.csv", victim=root/"victim.txt"; put(victim,"sentinel");
  std::error_code ec; fs::create_symlink(victim,p.string()+".tmp",ec); CHECK(!ec);
  std::unique_ptr<FileBatchWriter> w(create_csv_writer({}));
  CHECK(w->open(p.string(),e)==0); CHECK(w->write_row({text("new")},e)==0); CHECK(w->close(e)==0);
  CHECK(get(victim)=="sentinel"); CHECK(fs::is_symlink(p.string()+".tmp")); return true;
}
static bool target_created_during_export() {
  std::string e; const auto p=root/"raced.csv";
  std::unique_ptr<FileBatchWriter> w(create_csv_writer({})); CHECK(w->open(p.string(),e)==0);
  CHECK(w->write_row({text("new")},e)==0); put(p,"other writer");
  CHECK(w->close(e)!=0); CHECK(get(p)=="other writer"); return true;
}
static bool concurrent_writers() {
  std::string e; const auto p=root/"concurrent.csv";
  std::unique_ptr<FileBatchWriter> a(create_csv_writer({})), b(create_csv_writer({}));
  CHECK(a->open(p.string(),e)==0); CHECK(b->open(p.string(),e)==0);
  CHECK(a->write_row({text("first")},e)==0); CHECK(b->write_row({text("second")},e)==0);
  CHECK(a->close(e)==0); CHECK(b->close(e)!=0); CHECK(get(p)=="first\n"); return true;
}
static bool abort_removes_only_owned_temporary() {
  std::string e; const auto p=root/"abort.csv"; put(p.string()+".tmp","sentinel");
  std::unique_ptr<FileBatchWriter> w(create_csv_writer({})); CHECK(w->open(p.string(),e)==0);
  CHECK(w->write_row({text("uncommitted")},e)==0); w->abort();
  CHECK(!fs::exists(p)); CHECK(get(p.string()+".tmp")=="sentinel"); return true;
}
static bool csv_roundtrip() {
  std::string e; const auto p=root/"roundtrip.csv"; CsvWriteOptions wo; wo.null_str="NULL";
  std::unique_ptr<FileBatchWriter> w(create_csv_writer(wo)); CHECK(w->open(p.string(),e)==0);
  Cell null; std::vector<Cell> input={text("中文"),text("a,b"),text("say \"hi\""),text("line\r\nbreak"),text(""),text("NULL"),null};
  CHECK(w->write_row(input,e)==0); CHECK(w->close(e)==0);
  FileScanOptions ro; ro.header=false; ro.null_str="NULL";
  std::unique_ptr<FileBatchReader> r(create_file_reader(FILE_FORMAT_CSV)); CHECK(r->open(p.string(),ro,e)==0);
  std::vector<std::vector<Cell>> rows; CHECK(r->read_batch(2,rows,e)==0); CHECK(rows.size()==1); CHECK(rows[0].size()==input.size());
  for (size_t i=0;i<input.size();++i) { CHECK(rows[0][i].is_null==input[i].is_null); if(!input[i].is_null) CHECK(rows[0][i].str_value==input[i].str_value); }
  CHECK(r->read_batch(2,rows,e)==FileBatchReader::EOF_REACHED); return true;
}
static bool malformed_csv_suffix() {
  const auto p=root/"suffix.csv"; put(p,"id,name\n1,\"abc\"garbage\n");
  std::string e; std::vector<FileColumn> c; FileIdentity i; CHECK(infer_csv_schema(p.string(),{},c,i,e)!=0); CHECK(!e.empty()); return true;
}
static bool unterminated_csv_escape() {
  const auto p=root/"escape.csv"; put(p,"value\n\"abc\\");
  FileScanOptions o; o.escape='\\'; std::string e; std::vector<FileColumn> c; FileIdentity i;
  CHECK(infer_csv_schema(p.string(),o,c,i,e)!=0); CHECK(!e.empty()); return true;
}
static bool json_trailing_comma() {
  const auto p=root/"trailing.json"; put(p,"[{\"id\":1},]");
  std::string e; std::vector<FileColumn> c; FileIdentity i; CHECK(infer_json_schema(p.string(),{},c,i,e)!=0); CHECK(!e.empty()); return true;
}
static bool json_union() {
  const auto p=root/"union.jsonl"; put(p,"{\"id\":1,\"name\":\"一\"}\n{\"id\":2,\"score\":2.5}\n");
  std::string e; FileScanOptions o; o.format=FILE_FORMAT_JSON;
  std::unique_ptr<FileBatchReader> r(create_file_reader(FILE_FORMAT_JSON)); CHECK(r->open(p.string(),o,e)==0); CHECK(r->columns().size()==3);
  std::vector<std::vector<Cell>> rows; CHECK(r->read_batch(1,rows,e)==0); CHECK(rows.size()==1); CHECK(rows[0][2].is_null);
  rows.clear(); CHECK(r->read_batch(1,rows,e)==0); CHECK(rows[0][1].is_null); CHECK(rows[0][2].double_value==2.5); return true;
}
static bool csv_column_count() {
  for (const std::string &s : {"a,b\n1,2,3\n", "a,b\n1\n"}) {
    const auto p=root/"bad-count.csv"; put(p,s); std::string e; std::vector<FileColumn> c; FileIdentity i;
    CHECK(infer_csv_schema(p.string(),{},c,i,e)!=0); CHECK(e.find("columns")!=std::string::npos);
  }
  return true;
}
static bool changed_csv_scan() {
  const auto p=root/"changed.csv"; put(p,"id\n1\n2\n"); std::string e;
  std::unique_ptr<FileBatchReader> r(create_file_reader(FILE_FORMAT_CSV)); CHECK(r->open(p.string(),{},e)==0);
  std::vector<std::vector<Cell>> rows; CHECK(r->read_batch(1,rows,e)==0);
  put(p,"id\n1\n2\n300\n"); rows.clear(); CHECK(r->read_batch(1,rows,e)==FileBatchReader::FAILED); CHECK(rows.empty()); return true;
}
static bool changed_json_scan() {
  const auto p=root/"changed.jsonl"; put(p,"{\"id\":1}\n{\"id\":2}\n"); std::string e; FileScanOptions o; o.format=FILE_FORMAT_JSON;
  std::unique_ptr<FileBatchReader> r(create_file_reader(FILE_FORMAT_JSON)); CHECK(r->open(p.string(),o,e)==0);
  std::vector<std::vector<Cell>> rows; CHECK(r->read_batch(1,rows,e)==0);
  put(p,"{\"id\":300}\n"); rows.clear(); CHECK(r->read_batch(1,rows,e)==FileBatchReader::FAILED); CHECK(rows.empty()); return true;
}
static bool reader_reopen() {
  const auto p=root/"reopen.csv"; put(p,"id\n1\n"); std::string e;
  std::unique_ptr<FileBatchReader> r(create_file_reader(FILE_FORMAT_CSV)); CHECK(r->open(p.string(),{},e)==0);
  std::vector<std::vector<Cell>> rows; CHECK(r->read_batch(2,rows,e)==0); CHECK(r->read_batch(2,rows,e)==FileBatchReader::EOF_REACHED);
  CHECK(r->open(p.string(),{},e)==0); rows.clear(); CHECK(r->read_batch(2,rows,e)==0); CHECK(rows.size()==1); return true;
}
static bool parquet_scan() {
  if (fixture.empty()) return false;
  const auto p=root/"fixture.parquet"; put(p,get(fixture));
  FileScanOptions o; o.format=FILE_FORMAT_PARQUET; std::string e;
  std::unique_ptr<FileBatchReader> r(create_file_reader(FILE_FORMAT_PARQUET)); CHECK(r->open(p.string(),o,e)==0);
  int64_t count=0,sum=0; std::vector<std::vector<Cell>> rows;
  for (;;) { rows.clear(); int rc=r->read_batch(1,rows,e); CHECK(rc==0 || rc==1); for(const auto &row:rows) { CHECK(row.size()==1); CHECK(!row[0].is_null); sum+=row[0].int_value; ++count; } if(rc==1)break; }
  CHECK(count==7); CHECK(sum==21); return true;
}
static bool changed_parquet_scan() {
  const auto p=root/"changed.parquet"; put(p,get(fixture)); FileScanOptions o; o.format=FILE_FORMAT_PARQUET; std::string e;
  std::unique_ptr<FileBatchReader> r(create_file_reader(FILE_FORMAT_PARQUET)); CHECK(r->open(p.string(),o,e)==0);
  std::vector<std::vector<Cell>> rows; CHECK(r->read_batch(1,rows,e)==0);
  std::ofstream(p,std::ios::binary|std::ios::app)<<"changed"; rows.clear(); CHECK(r->read_batch(1,rows,e)==FileBatchReader::FAILED); CHECK(rows.empty()); return true;
}
static bool secure_path_confinement() {
  const auto allowed=root/"allowed", outside=root/"outside", sibling=root/"allowed-other";
  fs::create_directory(allowed); fs::create_directory(outside); fs::create_directory(sibling);
  put(allowed/"ok.csv","id\n1\n"); put(outside/"secret.csv","private"); put(sibling/"bad.csv","private");
  std::string p,e;
  CHECK(validate_file_path((allowed/"ok.csv").string(),allowed.string(),false,p,e)); CHECK(fs::path(p)==fs::canonical(allowed/"ok.csv"));
  CHECK(!validate_file_path((allowed/".."/"outside"/"secret.csv").string(),allowed.string(),false,p,e));
  CHECK(!validate_file_path((sibling/"bad.csv").string(),allowed.string(),false,p,e));
  CHECK(!validate_file_path((allowed/"ok.csv").string(),"",false,p,e));
  CHECK(!validate_file_path((allowed/"ok.csv").string(),(root/"missing").string(),false,p,e));
  CHECK(!validate_file_path(allowed.string(),allowed.string(),false,p,e));
  std::string nul=(allowed/"ok.csv").string(); nul.push_back('\0'); nul+="other";
  CHECK(!validate_file_path(nul,allowed.string(),false,p,e)); return true;
}
static bool secure_symlink_escape() {
  const auto allowed=root/"links", outside=root/"link-outside"; fs::create_directory(allowed); fs::create_directory(outside);
  put(outside/"secret.csv","private"); fs::create_symlink(outside/"secret.csv",allowed/"bad.csv"); fs::create_directory_symlink(outside,allowed/"bad-dir");
  std::string p,e;
  CHECK(!validate_file_path((allowed/"bad.csv").string(),allowed.string(),false,p,e));
  CHECK(!validate_file_path((allowed/"bad-dir"/"*.csv").string(),allowed.string(),true,p,e)); return true;
}
static bool secure_glob_prefix() {
  const auto allowed=root/"glob"; fs::create_directory(allowed); std::string p,e;
  CHECK(validate_file_path((allowed/"*.csv").string(),allowed.string(),true,p,e));
  CHECK(!validate_file_path((allowed/"*"/".."/"secret.csv").string(),allowed.string(),true,p,e));
  CHECK(!validate_file_path((root/"*"/"*.csv").string(),allowed.string(),true,p,e)); return true;
}
int main(int argc,char **argv) {
  if(argc!=3) return 2; root=fs::absolute(argv[1]); fixture=fs::absolute(argv[2]); std::error_code ec;
  if(!fs::create_directory(root,ec)) { std::cerr<<"test directory must not exist\n"; return 2; }
  int failed=0;
#define RUN(t) do { bool ok=t(); std::cout<<#t<<": "<<(ok?"PASS":"FAIL")<<"\n"; failed+=!ok; } while(0)
  RUN(existing_target); RUN(preexisting_temporary_file); RUN(temporary_symlink); RUN(target_created_during_export); RUN(concurrent_writers); RUN(abort_removes_only_owned_temporary); RUN(csv_roundtrip); RUN(malformed_csv_suffix); RUN(unterminated_csv_escape); RUN(json_trailing_comma); RUN(json_union); RUN(csv_column_count); RUN(changed_csv_scan); RUN(changed_json_scan); RUN(reader_reopen); RUN(parquet_scan); RUN(changed_parquet_scan); RUN(secure_path_confinement); RUN(secure_symlink_escape); RUN(secure_glob_prefix);
  fs::remove_all(root,ec); std::cout<<"failed="<<failed<<"\n"; return failed?1:0;
}
