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

// Regression coverage for AI provider boundary validation.
// Requires seekdb's normal GoogleTest/oblib/SQL test link environment.
#include <gtest/gtest.h>
#include <limits>
#include "lib/allocator/page_arena.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
using namespace oceanbase::common;
TEST(AIResponseValidation, EmbeddingRejectsNonFiniteDouble)
{
  ObArenaAllocator allocator;
  const double invalid[] = {std::numeric_limits<double>::infinity(),
                            -std::numeric_limits<double>::infinity(),
                            std::numeric_limits<double>::quiet_NaN()};
  for (double value : invalid) {
    ObJsonArray array(&allocator);
    ObJsonDouble number(value);
    ASSERT_EQ(OB_SUCCESS, array.append(&number));
    int64_t dimension = -1;
    EXPECT_EQ(OB_INVALID_DATA, ObAIFuncUtils::check_embedding_vector(array, dimension));
    EXPECT_EQ(0, dimension);
  }
}
TEST(AIResponseValidation, EmbeddingAcceptsFiniteNumbers)
{
  ObArenaAllocator allocator;
  ObJsonArray array(&allocator);
  ObJsonDouble first(0.125);
  ObJsonInt second(-2);
  ASSERT_EQ(OB_SUCCESS, array.append(&first));
  ASSERT_EQ(OB_SUCCESS, array.append(&second));
  int64_t dimension = -1;
  EXPECT_EQ(OB_SUCCESS, ObAIFuncUtils::check_embedding_vector(array, dimension));
  EXPECT_EQ(2, dimension);
}
TEST(AIResponseValidation, ConfigMustBeAnObject)
{
  ObArenaAllocator allocator;
  const char *invalid[] = {"[]", "[1]", "null", "1", "true", "\"text\""};
  for (const char *input : invalid) {
    ObString text(input);
    ObJsonObject *object = nullptr;
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ObAIFuncJsonUtils::get_json_object_form_str(allocator, text, object));
    EXPECT_EQ(nullptr, object);
  }
  ObString text("{\"temperature\":0.2,\"max_tokens\":16}");
  ObJsonObject *object = nullptr;
  EXPECT_EQ(OB_SUCCESS,
            ObAIFuncJsonUtils::get_json_object_form_str(allocator, text, object));
  ASSERT_NE(nullptr, object);
  EXPECT_EQ(2, object->element_count());
}

TEST(AIResponseValidation, OpenAIEmbeddingRestoresRequestOrder)
{
  ObArenaAllocator allocator;
  ObString text("{\"data\":[{\"index\":1,\"embedding\":[2,3]},{\"index\":0,\"embedding\":[0,1]}]}");
  ObJsonObject *response = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObAIFuncJsonUtils::get_json_object_form_str(allocator, text, response));
  ObOpenAIUtils::ObOpenAIEmbed provider;
  ObIJsonBase *output = nullptr;
  ASSERT_EQ(OB_SUCCESS, provider.parse_output(allocator, response, output));
  ASSERT_NE(nullptr, output);
  auto *array = static_cast<ObJsonArray *>(output);
  ASSERT_EQ(2, array->element_count());
  auto *first = static_cast<ObJsonArray *>(array->get_value(0));
  auto *second = static_cast<ObJsonArray *>(array->get_value(1));
  EXPECT_EQ(0, first->get_value(0)->get_int());
  EXPECT_EQ(2, second->get_value(0)->get_int());
}
TEST(AIResponseValidation, OpenAIEmbeddingRejectsDuplicateIndexesAndMixedDimensions)
{
  ObArenaAllocator allocator;
  const char *invalid[] = {
    "{\"data\":[{\"index\":0,\"embedding\":[0,1]},{\"index\":0,\"embedding\":[2,3]}]}",
    "{\"data\":[{\"index\":0,\"embedding\":[0,1]},{\"index\":1,\"embedding\":[2]}]}",
    "{\"data\":[{\"index\":2,\"embedding\":[0,1]}]}",
    "{\"data\":[{\"embedding\":[true]}]}"
  };
  for (const char *input : invalid) {
    ObString text(input);
    ObJsonObject *response = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObAIFuncJsonUtils::get_json_object_form_str(allocator, text, response));
    ObOpenAIUtils::ObOpenAIEmbed provider;
    ObIJsonBase *output = nullptr;
    EXPECT_EQ(OB_INVALID_DATA, provider.parse_output(allocator, response, output));
  }
}

TEST(AIResponseValidation, MissingResponseDoesNotCrashErrorFormatting)
{
  ObArenaAllocator allocator;
  ObString output;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObAIFuncJsonUtils::print_json_to_str(allocator, nullptr, output));
}
