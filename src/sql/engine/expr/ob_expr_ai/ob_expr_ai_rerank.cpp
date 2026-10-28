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
#include "ob_expr_ai_rerank.h"
#include <cmath>
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_sort.h"
#include "common/json_type/ob_json_common.h"
 
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase 
{
namespace sql 
{

ObExprAIRerank::ObExprAIRerank(common::ObIAllocator &alloc)
: ObFuncExprOperator(alloc, 
                    T_FUN_SYS_AI_RERANK, 
                    N_AI_RERANK, 
                    MORE_THAN_ZERO,
                    NOT_VALID_FOR_GENERATED_COL, 
                    NOT_ROW_DIMENSION) 
{
}

ObExprAIRerank::~ObExprAIRerank() 
{
}

int ObExprAIRerank::calc_result_typeN(ObExprResType &type,
                                    ObExprResType *types_stack,
                                    int64_t param_num,
                                    common::ObExprTypeCtx &type_ctx) const {
  UNUSED(type_ctx);
  UNUSED(types_stack);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 3 || param_num > 4)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    if (!ob_is_string_tc(types_stack[MODEL_IDX].get_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, model key must be string type");
    } else if (!ob_is_string_tc(types_stack[QUERY_IDX].get_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, query must be string type");
    } else {
      types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      types_stack[QUERY_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
    ObObjType in_type = types_stack[DOCUMENTS_IDX].get_type();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, DOCUMENTS_IDX, N_AI_RERANK))) {
    } else if (ob_is_string_type(in_type) && types_stack[DOCUMENTS_IDX].get_collation_type() != CS_TYPE_BINARY) {
      if (types_stack[DOCUMENTS_IDX].get_charset_type() != CHARSET_UTF8MB4) {
        types_stack[DOCUMENTS_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (param_num == 4) {
      ObObjType doc_key_type = types_stack[DOC_KEY_IDX].get_type();
      if (doc_key_type == ObNullType) {
      } else if (doc_key_type == ObJsonType) {
      } else if (ob_is_string_type(doc_key_type)) {
        types_stack[DOC_KEY_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      } else {  
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, doc key must be string type");
      }
    }

    if (OB_SUCC(ret)) {
      type.set_json();
      type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
    }
  }
  return ret;
}

int ObExprAIRerank::eval_ai_rerank(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) 
{
  INIT_SUCC(ret);
  ObDatum *arg_model_id = nullptr;
  ObDatum *arg_query = nullptr;
  ObDatum *arg_documents = nullptr;
  ObDatum *arg_doc_key = nullptr;
  if (OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_query, arg_documents, arg_doc_key))) {
  } else if (arg_model_id->is_null() || arg_query->is_null() || arg_documents->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, model id or query or documents is null");
    res.set_null();
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator());
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(N_AI_RERANK));
    ObAIFuncExprInfo *info = nullptr;
    share::ObAiModelEndpointInfo resolved_endpoint;
    const share::ObAiModelEndpointInfo *endpoint_info = &resolved_endpoint;
    query::ObIAiEndpointResolver *endpoint_resolver =
        ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
    ObString model_id = arg_model_id->get_string();
    ObString query = arg_query->get_string();
    ObJsonArray *document_array = nullptr;
    ObJsonArray *result_array = nullptr;
    ObIJsonBase *j_base = nullptr;
    bool is_null_result = false;
    // A NULL doc key means the documents are plain strings.
    const bool has_doc_key = OB_NOT_NULL(arg_doc_key) && !arg_doc_key->is_null();
    if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, DOCUMENTS_IDX,
          j_base, is_null_result))) {
    } else if (OB_ISNULL(j_base)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (j_base->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, document_array is not array");
    } else if (OB_ISNULL(document_array = static_cast<ObJsonArray *>(j_base))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (document_array->element_count() == 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, document_array is empty");
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < document_array->element_count(); i++) {
        ObJsonNode *doc_node = document_array->get_value(i);
        if (OB_ISNULL(doc_node)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (has_doc_key && doc_node->json_type() != ObJsonNodeType::J_OBJECT) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, document_array element is not object");
        } else if (!has_doc_key && doc_node->json_type() != ObJsonNodeType::J_STRING) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_rerank, document_array element is not string");
        }
      }
    }
    
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(temp_allocator, model_id, info))) {
    } else if (OB_ISNULL(endpoint_resolver)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(endpoint_resolver->resolve_by_model_name(
                   model_id, temp_allocator, resolved_endpoint))) {
    } else if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ObAIFuncUtils::check_info_type_rerank(info))) {
    } else {
      ObJsonArray *input_documents = document_array;
      ObArray<RerankScore> scores;
      if (has_doc_key) {
        ObJsonArray *doc_texts = nullptr;
        ObString doc_key = arg_doc_key->get_string();
        if (OB_FAIL(get_doc_array_from_documents_array_with_key(temp_allocator,
                                                                document_array,
                                                                doc_key,
                                                                doc_texts))) {
        } else {
          input_documents = doc_texts;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(collect_rerank_scores(temp_allocator, *info, *endpoint_info,
                                               query, input_documents, scores))) {
      } else if (OB_FAIL(check_score_coverage(scores, input_documents->element_count()))) {
      } else if (OB_FAIL(build_rerank_result(temp_allocator, document_array, scores,
                                             has_doc_key, result_array))) {
      } else {
        ObString raw_str;
        if (OB_FAIL(result_array->get_raw_binary(raw_str, &temp_allocator))) {
        } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_str))) {
        }
      }
    }
  }
  return ret;
}
int ObExprAIRerank::get_doc_array_from_documents_array_with_key(ObIAllocator &allocator, ObJsonArray *document_object_array, ObString &doc_key, ObJsonArray *&doc_array)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(document_object_array)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (doc_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObJsonArray *doc_array_obj = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, doc_array_obj))) {
    } else {
      int64_t count = document_object_array->element_count();
      ObJsonNode *doc_node = nullptr;
      ObJsonNode *doc_value_node = nullptr;
      ObJsonObject *doc_obj = nullptr;
      for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
        doc_node = document_object_array->get_value(i);
        if (OB_ISNULL(doc_node)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR (OB_INVALID_ARGUMENT, "ai_rerank, document_array element is null");
        } else if (doc_node->json_type() != ObJsonNodeType::J_OBJECT) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR (OB_INVALID_ARGUMENT, "ai_rerank, document_array element is not object");
        } else {
          doc_obj = static_cast<ObJsonObject *>(doc_node);
          doc_value_node = doc_obj->get_value(doc_key);
          if (OB_ISNULL(doc_value_node)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR (OB_INVALID_ARGUMENT, "ai_rerank, the value for given doc key is null");
          } else if (doc_value_node->json_type() != ObJsonNodeType::J_STRING) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR (OB_INVALID_ARGUMENT, "ai_rerank, the value for given doc key is not string");
          } else if (OB_FAIL(doc_array_obj->append(doc_value_node))) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      doc_array = doc_array_obj;
    }
  }
  return ret;
}

int ObExprAIRerank::construct_batch_document_array(ObIAllocator &allocator, ObJsonArray *document_array, int64_t start_idx, int64_t end_idx, ObJsonArray *&batch_document_array)
{
  INIT_SUCC(ret);
  ObJsonArray *result_array = nullptr;
  if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, result_array))) {
  } else {
    for (int64_t i = start_idx; i < end_idx; i++) {
      if (OB_FAIL(result_array->append(document_array->get_value(i)))) {
      }
    }
  }
  if (OB_SUCC(ret)) {
    batch_document_array = result_array;
  }
  return ret;
}

int ObExprAIRerank::inner_eval_ai_rerank(ObIAllocator &allocator, 
                                          const ObAIFuncExprInfo &info, 
                                          const share::ObAiModelEndpointInfo &endpoint_info,
                                          ObArray<ObString> &header_array, 
                                          ObString &query, 
                                          ObJsonArray *document_array, 
                                          ObJsonArray *&result_array)
{
  INIT_SUCC(ret);
  ObJsonArray *res = nullptr;
  ObJsonObject *config_json = nullptr;
  ObJsonObject *body = nullptr;
  ObJsonObject *http_response = nullptr;
  ObIJsonBase *response = nullptr;
  ObAIFuncClient ai_client;
  if (OB_FAIL(ObAIFuncUtils::get_rerank_body(allocator, info, endpoint_info, query, document_array, config_json, body))) {
  } else if (OB_FAIL(ai_client.send_post(allocator, endpoint_info.get_url(), header_array, body, http_response))) {
  } else if (OB_FAIL(ObAIFuncUtils::parse_rerank_output(allocator, endpoint_info, http_response, response))) {
  } else if (response->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(res = static_cast<ObJsonArray *>(response))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    result_array = res;
  }
  return ret;
}

// A relevance score may arrive as int, uint, double or decimal json number.
static int json_score_to_double(ObJsonNode *node, double &score)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!ObAIFuncJsonUtils::is_json_number(*node)) {
    ret = OB_INVALID_DATA;
  } else if (OB_FAIL(node->to_double(score))) {
    ret = OB_INVALID_DATA;
  }
  if (OB_SUCC(ret) && !std::isfinite(score)) {
    ret = OB_INVALID_DATA;
  }
  return ret;
}

int ObExprAIRerank::collect_rerank_scores(common::ObIAllocator &allocator,
                                          const common::ObAIFuncExprInfo &info,
                                          const share::ObAiModelEndpointInfo &endpoint_info,
                                          common::ObString &query,
                                          common::ObJsonArray *document_array,
                                          common::ObArray<RerankScore> &scores)
{
  INIT_SUCC(ret);
  ObArray<ObString> header_array;
  const int64_t document_count = OB_ISNULL(document_array) ? 0 : document_array->element_count();
  if (document_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObAIFuncUtils::get_header(allocator, info, endpoint_info, header_array))) {
  } else {
    for (int64_t batch_begin = 0; OB_SUCC(ret) && batch_begin < document_count;
         batch_begin += ObAIFuncUtils::MAX_RERANK_BATCH_DOCUMENTS) {
      int64_t batch_end = batch_begin + ObAIFuncUtils::MAX_RERANK_BATCH_DOCUMENTS;
      if (batch_end > document_count) {
        batch_end = document_count;
      }
      ObJsonArray *batch_document_array = NULL;
      ObJsonArray *batch_result_array = NULL;
      if (OB_FAIL(construct_batch_document_array(allocator, document_array, batch_begin,
                                                 batch_end, batch_document_array))) {
      } else if (OB_FAIL(inner_eval_ai_rerank(allocator, info, endpoint_info, header_array,
                                              query, batch_document_array, batch_result_array))) {
      } else if (OB_FAIL(append_batch_scores(allocator, batch_result_array, batch_begin,
                                             batch_end - batch_begin, scores))) {
      }
    }
  }
  return ret;
}

int ObExprAIRerank::append_batch_scores(common::ObIAllocator &allocator,
                                        common::ObJsonArray *batch_result_array,
                                        const int64_t batch_start,
                                        const int64_t batch_size,
                                        common::ObArray<RerankScore> &scores)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(batch_result_array)) {
    ret = OB_INVALID_DATA;
  } else if (batch_result_array->element_count() != batch_size) {
    // The model has to score every document of the batch, otherwise the caller
    // cannot tell which document the missing entry belongs to.
    ret = OB_INVALID_DATA;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      ObJsonNode *item = batch_result_array->get_value(i);
      ObJsonObject *item_obj = nullptr;
      ObJsonNode *index_node = nullptr;
      ObJsonNode *score_node = nullptr;
      double score = 0;
      int64_t local_index = i;
      if (OB_ISNULL(item) || item->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_DATA;
      } else if (OB_FALSE_IT(item_obj = static_cast<ObJsonObject *>(item))) {
      } else if (OB_ISNULL(score_node = item_obj->get_value(SCORE_KEY))) {
        // Some providers only name the score field "score".
        score_node = item_obj->get_value("score");
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(score_node)) {
          ret = OB_INVALID_DATA;
        } else if (OB_FAIL(json_score_to_double(score_node, score))) {
        } else if (OB_NOT_NULL(index_node = item_obj->get_value(INDEX_KEY))) {
          if (index_node->json_type() == ObJsonNodeType::J_INT) {
            local_index = static_cast<ObJsonInt *>(index_node)->value();
          } else if (index_node->json_type() == ObJsonNodeType::J_UINT) {
            local_index = static_cast<int64_t>(static_cast<ObJsonUint *>(index_node)->value());
          } else {
            ret = OB_INVALID_DATA;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (local_index < 0 || local_index >= batch_size) {
          ret = OB_INVALID_DATA;
        } else if (OB_FAIL(scores.push_back(RerankScore(batch_start + local_index,
                                                        score, score_node)))) {
        }
      }
    }
  }
  return ret;
}

int ObExprAIRerank::check_score_coverage(common::ObArray<RerankScore> &scores,
                                         const int64_t document_count)
{
  int ret = OB_SUCCESS;
  if (document_count <= 0 || scores.count() != document_count) {
    ret = OB_INVALID_DATA;
  } else {
    // After sorting by index a complete result is exactly 0..count-1, which
    // also proves that no document was mapped twice.
    lib::ob_sort(scores.begin(), scores.end(), RerankIndexAscCmp());
    for (int64_t i = 0; OB_SUCC(ret) && i < scores.count(); i++) {
      if (scores.at(i).index_ != i) {
        ret = OB_INVALID_DATA;
      }
    }
  }
  return ret;
}

int ObExprAIRerank::build_rerank_result(common::ObIAllocator &allocator,
                                        common::ObJsonArray *document_array,
                                        common::ObArray<RerankScore> &scores,
                                        const bool with_document,
                                        common::ObJsonArray *&result_array)
{
  INIT_SUCC(ret);
  ObJsonArray *out_array = nullptr;
  lib::ob_sort(scores.begin(), scores.end(), RerankScoreDescCmp());
  if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(allocator, out_array))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < scores.count(); i++) {
      const int64_t document_index = scores.at(i).index_;
      if (with_document) {
        ObJsonNode *doc_node = document_array->get_value(document_index);
        ObJsonObject *doc_obj = nullptr;
        if (OB_ISNULL(doc_node) || doc_node->json_type() != ObJsonNodeType::J_OBJECT) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_ISNULL(doc_obj = static_cast<ObJsonObject *>(doc_node->clone(&allocator, true)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_FAIL(doc_obj->add(MODEL_SCORE_KEY, scores.at(i).score_node_))) {
        } else if (OB_FAIL(out_array->append(doc_obj))) {
        }
      } else {
        ObJsonObject *entry_obj = nullptr;
        ObJsonInt *index_node = nullptr;
        if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, entry_obj))) {
        } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, document_index, index_node))) {
        } else if (OB_FAIL(entry_obj->add(INDEX_KEY, index_node))) {
        } else if (OB_FAIL(entry_obj->add(SCORE_KEY, scores.at(i).score_node_))) {
        } else if (OB_FAIL(out_array->append(entry_obj))) {
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    result_array = out_array;
  }
  return ret;
}
int ObExprAIRerank::construct_config_json(ObIAllocator &allocator, int64_t top_n, int64_t return_doc, ObJsonObject *&config_json)
{
  INIT_SUCC(ret);
  ObJsonObject *config_obj = nullptr;
  ObJsonInt *top_n_obj = nullptr;
  ObJsonBoolean *return_doc_obj = nullptr;
  bool return_doc_value = true;
  if (return_doc <= 0) {
    return_doc_value = false;
  }
  if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config_obj))) {
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_boolean(allocator, return_doc_value, return_doc_obj))) {
  } else if (OB_FAIL(config_obj->add("return_documents", return_doc_obj))) {
  } else if (top_n > 0) {
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, top_n, top_n_obj))) {
    } else if (OB_FAIL(config_obj->add("top_n", top_n_obj))) {
    }
  }
  if (OB_SUCC(ret)) {
    config_json = config_obj;
  }
  return ret;
}

int ObExprAIRerank::cg_expr(ObExprCGCtx &expr_cg_ctx, 
                        const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const 
{
  int ret = OB_SUCCESS;
  // TODO: support schema version match in plan cache for ai func
  // const ObRawExpr *model_key = raw_expr.get_param_expr(0);
  // if (OB_NOT_NULL(model_key)
  //     && (model_key->is_static_scalar_const_expr() || model_key->is_const_expr())
  //     && model_key->get_expr_type() != T_OP_GET_USER_VAR &&
  //     OB_NOT_NULL(expr_cg_ctx.schema_guard_)) {
  //   ObIAllocator *allocator = expr_cg_ctx.allocator_;
  //   ObExecContext *exec_ctx = expr_cg_ctx.session_->get_cur_exec_ctx();
  //   bool got_data = false;
  //   ObObj const_data;
  //   ObAIFuncExprInfo *info = nullptr;
  //   if (OB_ISNULL(allocator)) {
  //     ret = OB_ERR_UNEXPECTED;
  //     LOG_WARN("allocator is null", K(ret));
  //   } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
  //                                                         model_key,
  //                                                         const_data,
  //                                                         got_data,
  //                                                         *allocator))) {
  //     LOG_WARN("failed to calc offset expr", K(ret));
  //   } else if (!got_data || const_data.is_null()) {
  //   } else {
  //     ObAIFuncExprInfo *info = OB_NEWx(ObAIFuncExprInfo, allocator, *allocator, T_FUN_SYS_AI_RERANK);
  //     if (OB_ISNULL(info)) {
  //       ret = OB_ALLOCATE_MEMORY_FAILED;
  //       LOG_WARN("failed to allocate memory for ai rerank info", K(ret));
  //     } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(*allocator, const_data.get_string(), *expr_cg_ctx.schema_guard_, info))) {
  //       LOG_WARN("failed to get ai func info", K(ret), K(const_data.get_string()));
  //     } else {
  //       rt_expr.extra_info_ = info;
  //     }
  //   }
  // }
  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprAIRerank::eval_ai_rerank;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
