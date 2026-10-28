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
static bool strict_json_strings() {
  const auto p=root/"strict-strings.jsonl";
  const std::vector<std::string> invalid={
    "{\"s\":\"raw\nnewline\"}\n", "{\"s\":\"raw\ttab\"}\n",
    "{\"s\":\"\\uD800\"}\n", "{\"s\":\"\\uDC00\"}\n",
    "{\"s\":\"\\uD800x\"}\n", "{\"s\":\"\\uD800\\u0041\"}\n",
    std::string("{\"s\":\"")+char(0xC0)+char(0x80)+"\"}\n",
    std::string("{\"s\":\"")+char(0xED)+char(0xA0)+char(0x80)+"\"}\n",
    std::string("{\"s\":\"")+char(0xF4)+char(0x90)+char(0x80)+char(0x80)+"\"}\n"};
  for(const auto &input:invalid) {
    put(p,input); std::string e; std::vector<FileColumn> c; FileIdentity id;
    CHECK(infer_json_schema(p.string(),{},c,id,e)!=0); CHECK(!e.empty());
  }
  put(p,"{\"s\":\"\\uD83D\\uDE00\\n\\t\"}\n");
  FileScanOptions o; o.format=FILE_FORMAT_JSON; std::string e;
  std::unique_ptr<FileBatchReader> r(create_file_reader(FILE_FORMAT_JSON)); CHECK(r->open(p.string(),o,e)==0);
  std::vector<std::vector<Cell>> rows; CHECK(r->read_batch(1,rows,e)==0);
  CHECK(rows.size()==1); CHECK(rows[0][0].str_value=="😀\n\t"); return true;
}
static bool strict_json_numbers() {
  const auto p=root/"strict-numbers.jsonl";
  for(const std::string &v:{"+1",".1","1.","01","-01","-.2","1e","1e+","00.1"}) {
    put(p,"{\"n\":"+v+"}\n"); std::string e; std::vector<FileColumn> c; FileIdentity id;
    CHECK(infer_json_schema(p.string(),{},c,id,e)!=0);
  }
  for(const std::string &v:{"0","-0","0.1","-0.1","1e+2","1.23e-2"}) {
    put(p,"{\"n\":"+v+"}\n"); std::string e; std::vector<FileColumn> c; FileIdentity id;
    CHECK(infer_json_schema(p.string(),{},c,id,e)==0);
  }
  return true;
}
static bool csv_utf8_validation() {
  const auto p=root/"invalid-utf8.csv";
  for(const std::string &v:{std::string(1,char(0x80)),std::string(1,char(0xC2)),std::string("\xF0\x80\x80\x80",4)}) {
    put(p,"value\n"+v+"\n"); std::string e; std::vector<FileColumn> c; FileIdentity id;
    CHECK(infer_csv_schema(p.string(),{},c,id,e)!=0); CHECK(e.find("UTF-8")!=std::string::npos);
  }
  return true;
}
static bool field_and_record_limits() {
  const auto p=root/"limited.csv"; std::string e; std::vector<FileColumn> c; FileIdentity id;
  FileScanOptions o; o.limits.max_field_bytes=4; o.limits.max_record_bytes=64;
  put(p,"col\n1234\n"); CHECK(infer_csv_schema(p.string(),o,c,id,e)==0);
  put(p,"col\n\"1234\"\n"); CHECK(infer_csv_schema(p.string(),o,c,id,e)==0);
  put(p,"col\n12345\n"); CHECK(infer_csv_schema(p.string(),o,c,id,e)!=0); CHECK(e.find("limit")!=std::string::npos);
  put(p,"col\n\"12\"\"34\"\n"); CHECK(infer_csv_schema(p.string(),o,c,id,e)!=0);
  o.limits.max_field_bytes=64; o.limits.max_record_bytes=8;
  put(p,"a,b\n1234,5678\n"); CHECK(infer_csv_schema(p.string(),o,c,id,e)!=0); CHECK(e.find("record")!=std::string::npos);
  const auto j=root/"limited.jsonl"; o.format=FILE_FORMAT_JSON; o.limits.max_field_bytes=4; o.limits.max_record_bytes=64;
  put(j,"{\"s\":\"1234\"}\n"); CHECK(infer_json_schema(j.string(),o,c,id,e)==0);
  put(j,"{\"s\":\"12345\"}\n"); CHECK(infer_json_schema(j.string(),o,c,id,e)!=0);
  put(j,"{\"s\":\"\\uD83D\\uDE00x\"}\n"); CHECK(infer_json_schema(j.string(),o,c,id,e)!=0);
  put(j,"{\"n\":12345}\n"); CHECK(infer_json_schema(j.string(),o,c,id,e)!=0);
  o.limits.max_field_bytes=64; o.limits.max_record_bytes=10;
  put(j,"{\"s\":\"long record\"}\n"); CHECK(infer_json_schema(j.string(),o,c,id,e)!=0); CHECK(e.find("record")!=std::string::npos);
  return true;
}
static bool schema_column_limits() {
  const auto p=root/"wide.csv", j=root/"wide.jsonl"; std::string e; std::vector<FileColumn> c; FileIdentity id;
  FileScanOptions o; o.limits.max_columns=2;
  put(p,"a,b,c\n1,2,3\n"); CHECK(infer_csv_schema(p.string(),o,c,id,e)!=0); CHECK(e.find("column")!=std::string::npos);
  put(j,"{\"a\":1,\"b\":2,\"c\":3}\n"); CHECK(infer_json_schema(j.string(),o,c,id,e)!=0);
  put(j,"{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n"); CHECK(infer_json_schema(j.string(),o,c,id,e)!=0); CHECK(e.find("union")!=std::string::npos);
  o.limits.max_columns=0; put(p,"a\n1\n"); CHECK(infer_csv_schema(p.string(),o,c,id,e)!=0);
  o.limits=FileScanLimits(); ++o.limits.max_field_bytes; CHECK(infer_csv_schema(p.string(),o,c,id,e)!=0);
  return true;
}
static bool schema_metadata_limits() {
  const auto p=root/"schema-metadata.jsonl"; std::string e; std::vector<FileColumn> c; FileIdentity id;
  FileScanOptions o; o.limits.max_record_bytes=10;
  put(p,"{\"abc\":1}\n{\"def\":2}\n");
  CHECK(infer_json_schema(p.string(),o,c,id,e)!=0);
  CHECK(c.empty()); CHECK(e.find("metadata")!=std::string::npos);
  o.limits.max_record_bytes=12;
  CHECK(infer_json_schema(p.string(),o,c,id,e)==0); CHECK(c.size()==2);
  return true;
}
static bool csv_json_sample_size_contract() {
  const auto csv=root/"sample-size.csv"; const auto json=root/"sample-size.jsonl";
  put(csv,"id\n1\n"); put(json,"{\"id\":1}\n");
  FileScanOptions o; o.sample_size=1; std::string e; std::vector<FileColumn> c; FileIdentity id;
  CHECK(infer_csv_schema(csv.string(),o,c,id,e)!=0); CHECK(e.find("sample_size")!=std::string::npos);
  o.format=FILE_FORMAT_JSON; CHECK(infer_json_schema(json.string(),o,c,id,e)!=0); CHECK(e.find("sample_size")!=std::string::npos);
  return true;
}
static bool batch_memory_limits() {
  const auto p=root/"batch-limit.csv",j=root/"batch-limit.jsonl"; std::string e;
  put(p,"a\n1\n2\n3\n"); FileScanOptions o; o.limits.max_batch_rows=2;
  std::unique_ptr<FileBatchReader> r(create_file_reader(FILE_FORMAT_CSV)); CHECK(r->open(p.string(),o,e)==0);
  std::vector<std::vector<Cell>> rows; CHECK(r->read_batch(1000000,rows,e)==0); CHECK(rows.size()==2);
  rows.clear(); CHECK(r->read_batch(1000000,rows,e)==0); CHECK(rows.size()==1);
  o.limits.max_batch_bytes=1; CHECK(r->open(p.string(),o,e)==0); rows.clear(); CHECK(r->read_batch(1,rows,e)==FileBatchReader::FAILED); CHECK(rows.empty());
  put(j,"{\"a\":1}\n");o.format=FILE_FORMAT_JSON;std::unique_ptr<FileBatchReader> jr(create_file_reader(FILE_FORMAT_JSON)); CHECK(jr->open(j.string(),o,e)==0); rows.clear(); CHECK(jr->read_batch(1,rows,e)==FileBatchReader::FAILED); CHECK(rows.empty());
  return true;
}
static bool same_size_replacement_detected() {
  const auto p=root/"replaced.csv",replacement=root/"replacement.csv"; put(p,"id\n1\n2\n"); std::string e;
  const auto stamp=fs::last_write_time(p); std::unique_ptr<FileBatchReader> r(create_file_reader(FILE_FORMAT_CSV)); CHECK(r->open(p.string(),{},e)==0);
  std::vector<std::vector<Cell>> rows; CHECK(r->read_batch(1,rows,e)==0);
  put(replacement,"id\n8\n9\n");fs::last_write_time(replacement,stamp);fs::rename(replacement,p);
  rows.clear(); CHECK(r->read_batch(1,rows,e)==FileBatchReader::FAILED); CHECK(rows.empty());
  return true;
}
static bool same_size_inplace_write_detected() {
  const auto p=root/"rewritten.jsonl"; put(p,"{\"id\":1}\n{\"id\":2}\n"); const auto stamp=fs::last_write_time(p);
  FileScanOptions o; o.format=FILE_FORMAT_JSON; std::string e;
  std::unique_ptr<FileBatchReader> r(create_file_reader(FILE_FORMAT_JSON)); CHECK(r->open(p.string(),o,e)==0);
  std::vector<std::vector<Cell>> rows; CHECK(r->read_batch(1,rows,e)==0);
  put(p,"{\"id\":8}\n{\"id\":9}\n");fs::last_write_time(p,stamp);rows.clear(); CHECK(r->read_batch(1,rows,e)==FileBatchReader::FAILED); CHECK(rows.empty()); return true;
}
static bool safe_open_rejects_symlink_components() {
  const auto d=root/"safe-open",inside=d/"inside";fs::create_directories(inside);put(inside/"data.csv","a\n1\n");
  fs::create_directory_symlink(inside,d/"dir-link");fs::create_symlink(inside/"data.csv",d/"file-link");
  std::FILE *fp=open_local_file_for_read((inside/"data.csv").string());CHECK(fp!=nullptr);std::fclose(fp);
  CHECK(open_local_file_for_read((d/"dir-link"/"data.csv").string())==nullptr);
  CHECK(open_local_file_for_read((d/"file-link").string())==nullptr);
  CHECK(open_local_file_for_read((inside/".."/"inside"/"data.csv").string())==nullptr);
  CHECK(open_local_file_for_read(inside.string())==nullptr);
  return true;
}
static bool csv_single_null_records() {
  const auto p=root/"single-null.csv"; put(p,"value\r\n\r\n\"\"\r\nx\r\n");std::string e;
  std::unique_ptr<FileBatchReader> r(create_file_reader(FILE_FORMAT_CSV));CHECK(r->open(p.string(),{},e)==0);
  std::vector<std::vector<Cell>> rows;CHECK(r->read_batch(8,rows,e)==0);CHECK(rows.size()==3);
  CHECK(rows[0][0].is_null);CHECK(!rows[1][0].is_null);CHECK(rows[1][0].str_value.empty());CHECK(rows[2][0].str_value=="x");
  const auto exported=root/"single-null-export.csv";std::unique_ptr<FileBatchWriter> w(create_csv_writer({}));CHECK(w->open(exported.string(),e)==0);
  CHECK(w->write_row({Cell()},e)==0);CHECK(w->write_row({text("")},e)==0);CHECK(w->close(e)==0);
  FileScanOptions o;o.header=false;CHECK(r->open(exported.string(),o,e)==0);rows.clear();CHECK(r->read_batch(8,rows,e)==0);CHECK(rows.size()==2);CHECK(rows[0][0].is_null);CHECK(!rows[1][0].is_null);return true;
}
static bool exact_field_limit_crlf() {
  const auto p=root/"exact-crlf.csv";put(p,"name\r\n1234\r\n");FileScanOptions o;o.limits.max_field_bytes=4;
  std::string e;std::vector<FileColumn> c;FileIdentity id;CHECK(infer_csv_schema(p.string(),o,c,id,e)==0);return true;
}
static bool glob_safe_enumeration() {
  const auto d=root/"glob-scan";fs::create_directories(d/"nested");put(d/"one.csv","");put(d/"two.json","");put(d/"nested"/"three.csv","");put(d/".hidden.csv","");
  std::string e;std::vector<std::string> files;
  CHECK(discover_files((d/"*.csv").string(),files,e)==0);CHECK(files.size()==1);CHECK(files[0]==(d/"one.csv").string());
  CHECK(discover_files((d/"*"/"*.csv").string(),files,e)==0);CHECK(files.size()==1);CHECK(files[0]==(d/"nested"/"three.csv").string());
  fs::create_directory_symlink(root,d/"linked");CHECK(discover_files((d/"*"/"*.csv").string(),files,e)!=0);CHECK(files.empty());CHECK(e.find("symbolic")!=std::string::npos);
  fs::remove(d/"linked");fs::create_symlink(d/"one.csv",d/"link.csv");CHECK(discover_files((d/"*.csv").string(),files,e)!=0);CHECK(files.empty());
  std::string depth=d.string();for(int i=0;i<70;++i)depth+="/a";depth+="/*.csv";CHECK(discover_files(depth,files,e)!=0);CHECK(e.find("depth")!=std::string::npos);return true;
}
static bool glob_match_limit() {
  const auto d=root/"glob-limit";fs::create_directory(d);
  for(int i=0;i<10001;++i)put(d/(std::to_string(i)+".csv"),"");
  std::string e;std::vector<std::string> files;CHECK(discover_files((d/"*.csv").string(),files,e)!=0);CHECK(files.empty());CHECK(e.find("limit")!=std::string::npos);return true;
}
static bool parquet_reader_resource_limits() {
  const auto p=root/"parquet-limit.parquet";put(p,get(fixture));FileScanOptions o;o.format=FILE_FORMAT_PARQUET;o.limits.max_batch_rows=2;
  std::string e;std::unique_ptr<FileBatchReader> r(create_file_reader(FILE_FORMAT_PARQUET));CHECK(r->open(p.string(),o,e)==0);
  std::vector<std::vector<Cell>> rows;CHECK(r->read_batch(1000000,rows,e)==0);CHECK(rows.size()==2);
  o.limits.max_batch_bytes=1;CHECK(r->open(p.string(),o,e)==0);rows.clear();CHECK(r->read_batch(1,rows,e)==FileBatchReader::FAILED);CHECK(rows.empty());CHECK(e.find("limit")!=std::string::npos);
  o.limits=FileScanLimits();o.limits.max_record_bytes=1;CHECK(r->open(p.string(),o,e)==0);rows.clear();CHECK(r->read_batch(1,rows,e)==FileBatchReader::FAILED);return true;
}
static bool parquet_same_size_replacement() {
  const auto p=root/"parquet-replace.parquet",replacement=root/"parquet-new.parquet";put(p,get(fixture));const auto stamp=fs::last_write_time(p);
  FileScanOptions o;o.format=FILE_FORMAT_PARQUET;std::string e;std::unique_ptr<FileBatchReader> r(create_file_reader(FILE_FORMAT_PARQUET));CHECK(r->open(p.string(),o,e)==0);
  std::vector<std::vector<Cell>> rows;CHECK(r->read_batch(1,rows,e)==0);put(replacement,get(fixture));fs::last_write_time(replacement,stamp);fs::rename(replacement,p);
  rows.clear();CHECK(r->read_batch(1,rows,e)==FileBatchReader::FAILED);CHECK(rows.empty());return true;
}
int main(int argc,char **argv) {
  if(argc!=3) return 2; root=fs::absolute(argv[1]); fixture=fs::absolute(argv[2]); std::error_code ec;
  if(!fs::create_directory(root,ec)) { std::cerr<<"test directory must not exist\n"; return 2; }
  root=fs::canonical(root); fixture=fs::canonical(fixture);
  int failed=0;
#define RUN(t) do { bool ok=t(); std::cout<<#t<<": "<<(ok?"PASS":"FAIL")<<"\n"; failed+=!ok; } while(0)
  RUN(existing_target); RUN(preexisting_temporary_file); RUN(temporary_symlink); RUN(target_created_during_export); RUN(concurrent_writers); RUN(abort_removes_only_owned_temporary); RUN(csv_roundtrip); RUN(malformed_csv_suffix); RUN(unterminated_csv_escape); RUN(json_trailing_comma); RUN(json_union); RUN(csv_column_count); RUN(changed_csv_scan); RUN(changed_json_scan); RUN(reader_reopen); RUN(parquet_scan); RUN(changed_parquet_scan); RUN(secure_path_confinement); RUN(secure_symlink_escape); RUN(secure_glob_prefix); RUN(strict_json_strings); RUN(strict_json_numbers); RUN(csv_utf8_validation); RUN(field_and_record_limits); RUN(schema_column_limits); RUN(schema_metadata_limits); RUN(csv_json_sample_size_contract); RUN(batch_memory_limits); RUN(same_size_replacement_detected); RUN(same_size_inplace_write_detected); RUN(safe_open_rejects_symlink_components); RUN(csv_single_null_records); RUN(exact_field_limit_crlf); RUN(glob_safe_enumeration); RUN(glob_match_limit); RUN(parquet_reader_resource_limits); RUN(parquet_same_size_replacement);
  fs::remove_all(root,ec); std::cout<<"failed="<<failed<<"\n"; return failed?1:0;
}
