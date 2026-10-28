/*
 * Copyright (c) 2025 OceanBase.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

// Runs in the normal GoogleTest/oblib schema test environment.
#include <gtest/gtest.h>
#include <algorithm>
#include <vector>
#include "share/schema/ob_column_schema.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "lib/hash/ob_hashmap.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace {
void initialize_ai_column(ObColumnSchemaV2 &column)
{
  column.set_table_id(50001);
  column.set_column_id(19);
  ASSERT_EQ(OB_SUCCESS, column.set_column_name("summary"));
  column.set_data_type(ObVarcharType);
  column.set_data_length(400);
  ASSERT_EQ(OB_SUCCESS, column.set_ai_column_expr("AI_COMPLETE('model',content)"));
  ASSERT_EQ(OB_SUCCESS, column.set_ai_dependency_ids("16,18"));
  column.set_ai_state_column_id(20);
  column.set_ai_request_column_id(21);
  column.set_ai_result_spec_column_id(22);
  column.set_ai_max_retries(4);
  column.set_ai_definition_version(100123);
  ObObj sql_mode;
  sql_mode.set_uint64(SMO_ANSI_QUOTES | SMO_NO_BACKSLASH_ESCAPES);
  ASSERT_EQ(OB_SUCCESS, column.get_local_session_var().set_local_var_capacity(1));
  ASSERT_EQ(OB_SUCCESS, column.get_local_session_var().add_local_var(
      oceanbase::share::SYS_VAR_SQL_MODE, sql_mode));
}
void expect_ai_column(const ObColumnSchemaV2 &column)
{
  EXPECT_TRUE(column.is_ai_column());
  EXPECT_FALSE(column.is_generated_column());
  EXPECT_FALSE(column.is_hidden());
  EXPECT_EQ(ObString("AI_COMPLETE('model',content)"), column.get_ai_column_expr());
  EXPECT_EQ(ObString("16,18"), column.get_ai_dependency_ids());
  EXPECT_EQ(20, column.get_ai_state_column_id());
  EXPECT_EQ(21, column.get_ai_request_column_id());
  EXPECT_EQ(22, column.get_ai_result_spec_column_id());
  EXPECT_EQ(4, column.get_ai_max_retries());
  EXPECT_EQ(100123, column.get_ai_definition_version());
  oceanbase::share::ObSessionSysVar *sql_mode = nullptr;
  ASSERT_EQ(OB_SUCCESS, column.get_local_session_var().get_local_var(
      oceanbase::share::SYS_VAR_SQL_MODE, sql_mode));
  ASSERT_NE(nullptr, sql_mode);
  EXPECT_EQ(static_cast<uint64_t>(SMO_ANSI_QUOTES | SMO_NO_BACKSLASH_ESCAPES), sql_mode->val_.get_uint64());
}
}

TEST(AIColumnSchema, AssignmentOwnsDefinitionMemory)
{
  ObColumnSchemaV2 copied;
  {
    ObColumnSchemaV2 source;
    initialize_ai_column(source);
    ASSERT_EQ(OB_SUCCESS, copied.assign(source));
    EXPECT_NE(source.get_ai_column_expr().ptr(), copied.get_ai_column_expr().ptr());
    EXPECT_NE(source.get_ai_dependency_ids().ptr(), copied.get_ai_dependency_ids().ptr());
    source.reset();
  }
  expect_ai_column(copied);
}

TEST(AIColumnSchema, SerializationOwnsDecodedMemory)
{
  ObColumnSchemaV2 decoded;
  {
    ObColumnSchemaV2 source;
    initialize_ai_column(source);
    std::vector<char> bytes(source.get_serialize_size());
    int64_t written = 0;
    ASSERT_EQ(OB_SUCCESS, source.serialize(bytes.data(), bytes.size(), written));
    ASSERT_EQ(bytes.size(), written);
    int64_t read = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.deserialize(bytes.data(), written, read));
    EXPECT_EQ(written, read);
    std::fill(bytes.begin(), bytes.end(), 0);
  }
  expect_ai_column(decoded);
}

TEST(AIColumnSchema, ColumnRemappingIncludesDependenciesAndPrivateColumns)
{
  ObColumnSchemaV2 column;
  initialize_ai_column(column);
  oceanbase::common::hash::ObHashMap<uint64_t, uint64_t> ids;
  ASSERT_EQ(OB_SUCCESS, ids.create(16, oceanbase::lib::ObLabel("AIColTest")));
  const uint64_t original[] = {16, 18, 19, 20, 21, 22};
  for (uint64_t id : original) ASSERT_EQ(OB_SUCCESS, ids.set_refactored(id, id + 100));
  ASSERT_EQ(OB_SUCCESS, column.convert_column_id(ids));
  EXPECT_EQ(119, column.get_column_id());
  EXPECT_EQ(ObString("116,118"), column.get_ai_dependency_ids());
  EXPECT_EQ(120, column.get_ai_state_column_id());
  EXPECT_EQ(121, column.get_ai_request_column_id());
  EXPECT_EQ(122, column.get_ai_result_spec_column_id());
  EXPECT_EQ(100123, column.get_ai_definition_version());
}
