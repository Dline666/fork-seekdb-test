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

#define USING_LOG_PREFIX STORAGE

#include "ob_text_daat_iter.h"

namespace oceanbase
{
namespace storage
{
namespace
{
// Each (token, document ID) posting is unique and streams are ordered by binary
// ID. Taking all prefixes <= the minimum loaded tail closes a window: unseen
// rows in every stream are strictly above its end, including across batches.
bool count_owned_binary_window(const ObDocIdExt *const *ids, const int64_t *remaining,
                               const int64_t streams, int64_t *take, int64_t &cardinality)
{
  const ObDatum *end = nullptr;
  auto compare = [](const ObDatum &left, const ObDatum &right) {
    if (left.is_null() || right.is_null()) {
      return left.is_null() ? (right.is_null() ? 0 : -1) : 1;
    }
    const int64_t length = MIN(left.len_, right.len_);
    const int prefix = length > 0 ? MEMCMP(left.ptr_, right.ptr_, length) : 0;
    return prefix != 0 ? prefix : (left.len_ < right.len_ ? -1 : (left.len_ > right.len_ ? 1 : 0));
  };
  for (int64_t i = 0; i < streams; ++i) {
    take[i] = 0;
    if (remaining[i] > 0) {
      const ObDatum &tail = ids[i][remaining[i] - 1].get_datum();
      if (end == nullptr || compare(tail, *end) < 0) {
        end = &tail;
      }
    }
  }
  if (end == nullptr) { return false; }
  int64_t records = 0;
  for (int64_t i = 0; i < streams; ++i) {
    int64_t left = 0;
    int64_t right = remaining[i];
    while (left < right) {
      const int64_t middle = left + (right - left) / 2;
      if (compare(ids[i][middle].get_datum(), *end) <= 0) {
        left = middle + 1;
      } else {
        right = middle;
      }
    }
    take[i] = left;
    records += left;
  }
  if (records < 128) { return false; }
  if (end->is_null() || end->len_ != 16 || end->ptr_ == nullptr) { return false; }
  const char *const anchor = end->ptr_;
  for (int64_t i = 0; i < streams; ++i) {
    for (int64_t j = 0; j < take[i]; ++j) {
      const ObDatum &id = ids[i][j].get_datum();
      if (id.is_null() || id.len_ != 16 || id.ptr_ == nullptr) { return false; }
      // Native generated IDs place the sequence's low bytes at offsets 8/9
      // on little-endian hosts. This is merely a compression opportunity:
      // validate all other bytes, making the two-byte key an exact identity
      // for arbitrary binary strings too, without assuming numeric ID order.
      if (MEMCMP(id.ptr_, anchor, 8) != 0
          || MEMCMP(id.ptr_ + 10, anchor + 10, 6) != 0) { return false; }
    }
  }
  // Only after validating the complete identity compression can ordered span
  // endpoints determine the bitmap range. end itself belongs to the window.
  uint32_t minimum = 65535;
  const unsigned char *end_bytes = reinterpret_cast<const unsigned char *>(anchor);
  const uint32_t maximum = (static_cast<uint32_t>(end_bytes[8]) << 8) | end_bytes[9];
  for (int64_t i = 0; i < streams; ++i) {
    if (take[i] > 0) {
      const unsigned char *first = reinterpret_cast<const unsigned char *>(ids[i][0].get_datum().ptr_);
      minimum = MIN(minimum, (static_cast<uint32_t>(first[8]) << 8) | first[9]);
    }
  }
  uint64_t bitmap[1024];
  const uint32_t first_word = minimum >> 6;
  const uint32_t last_word = maximum >> 6;
  MEMSET(bitmap + first_word, 0, (last_word - first_word + 1) * sizeof(uint64_t));
  for (int64_t i = 0; i < streams; ++i) {
    for (int64_t j = 0; j < take[i]; ++j) {
      const unsigned char *bytes = reinterpret_cast<const unsigned char *>(ids[i][j].get_datum().ptr_);
      const uint32_t key = (static_cast<uint32_t>(bytes[8]) << 8) | bytes[9];
      bitmap[key >> 6] |= uint64_t(1) << (key & 63);
    }
  }
  cardinality = 0;
  for (uint32_t i = first_word; i <= last_word; ++i) {
    cardinality += __builtin_popcountll(bitmap[i]);
  }
  return true;
}
} // namespace

int ObTextDaaTIter::init(const ObTextDaaTParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.base_param_) || OB_ISNULL(param.dim_iters_) || OB_ISNULL(param.allocator_)
      || OB_ISNULL(param.relevance_collector_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObSRDaaTIterImpl::init(*param.base_param_, *param.dim_iters_,
                                            *param.allocator_, *param.relevance_collector_))) {
  } else if (param.base_param_->need_project_relevance()
      && OB_FAIL(bm25_param_estimator_.init(param.bm25_param_est_ctx_))) {
  } else {
    mode_flag_ = param.mode_flag_;
    function_lookup_mode_ = param.function_lookup_mode_;
    minimum_should_match_ = param.minimum_should_match_;
    exhausted_mask_ = 0;
    pending_count_ = 0;
  }
  return ret;
}

void ObTextDaaTIter::reset()
{
  exhausted_mask_ = 0;
  pending_count_ = 0;
  bm25_param_estimator_.reset();
  // this class can know the type of dim_iters_, but ObSRDaaTIterImpl maybe not
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(i))->reset();
    }
  }
  ObSRDaaTIterImpl::reset();
}

void ObTextDaaTIter::reuse(const bool switch_tablet)
{
  exhausted_mask_ = 0;
  pending_count_ = 0;
  if ((OB_NOT_NULL(dim_iters_) && 0 == dim_iters_->count())) {
    // do nothing
  } else {
    bm25_param_estimator_.reuse(switch_tablet);
  }
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(i))->reuse();
    }
  }
  ObSRDaaTIterImpl::reuse(switch_tablet);
}

int ObTextDaaTIter::pre_process()
{
  int ret = OB_SUCCESS;
  if (dim_iters_->count() == 0) {
    ret = OB_ITER_END;
  } else if (iter_param_->need_project_relevance()) {
    if (OB_FAIL(bm25_param_estimator_.do_estimation(*iter_param_->eval_ctx_))) {
    }
  }
  return ret;
}

int ObTextDaaTIter::get_next_rows(const int64_t capacity, int64_t &count)
{
  if (!is_inited_ || capacity <= 0 || mode_flag_ != sql::NATURAL_LANGUAGE_MODE
      || function_lookup_mode_ || iter_param_->emit_domain_id_
      || iter_param_->relevance_expr_ != nullptr || iter_param_->need_project_relevance()
      || iter_param_->need_filter() || dim_iters_->count() == 0
      || dim_iters_->count() > ObSRSimpleMerger::USE_SIMPLE_MERGER_MAX_TABLE_CNT) {
    return ObSRDaaTIterImpl::get_next_rows(capacity, count);
  }
  int ret = OB_SUCCESS;
  count = 0;
  const common::ObLimitParam &limit = *iter_param_->limit_param_;
  const int64_t real_capacity = MIN(capacity, iter_param_->max_batch_size_);
  const int64_t dim_count = dim_iters_->count();
  enum class IdCompareKind { GENERIC, UNSIGNED_INTEGER, BINARY_STRING };
  const auto &id_meta = iter_param_->id_proj_expr_->datum_meta_;
  const IdCompareKind compare_kind = id_meta.type_ == common::ObUInt64Type
      ? IdCompareKind::UNSIGNED_INTEGER
      : ((id_meta.type_ == common::ObVarcharType || id_meta.type_ == common::ObCharType)
          && id_meta.cs_type_ == common::CS_TYPE_BINARY
          ? IdCompareKind::BINARY_STRING : IdCompareKind::GENERIC);
  constexpr int64_t MAX_STREAMS = ObSRSimpleMerger::USE_SIMPLE_MERGER_MAX_TABLE_CNT;
  ObTextRetrievalDaaTTokenIter *iters[MAX_STREAMS] = {};
  const ObDocIdExt *ids[MAX_STREAMS] = {};
  const ObDatum *heads[MAX_STREAMS] = {};
  int64_t remaining[MAX_STREAMS] = {};
  int64_t consumed[MAX_STREAMS] = {};
  for (int64_t i = 0; i < dim_count; ++i) {
    iters[i] = static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(i));
  }
  uint8_t refill_mask = ((1U << dim_count) - 1) & ~exhausted_mask_;
  bool try_window = true;
  auto refill = [&](const int64_t i) {
    if (OB_SUCC(ret) && (refill_mask & (1U << i))) {
      iters[i]->consume_owned_ids(consumed[i]);
      consumed[i] = 0;
      try_window = true;
      if (OB_FAIL(iters[i]->get_owned_id_span(ids[i], remaining[i]))) {
        if (ret == OB_ITER_END) {
          exhausted_mask_ |= (1U << i);
          ret = OB_SUCCESS;
        }
      }
      heads[i] = remaining[i] > 0 ? &ids[i]->get_datum() : nullptr;
      if (compare_kind == IdCompareKind::GENERIC) {
        iter_domain_ids_[i] = heads[i];
      }
    }
  };
  auto consume = [&](const int64_t i) {
    ++ids[i];
    --remaining[i];
    ++consumed[i];
    if (remaining[i] > 0) {
      heads[i] = &ids[i]->get_datum();
    } else {
      heads[i] = nullptr;
      refill_mask |= (1U << i);
    }
    if (compare_kind == IdCompareKind::GENERIC) {
      iter_domain_ids_[i] = heads[i];
    }
  };
  if (limit.is_valid() && output_row_cnt_ >= limit.limit_) {
    ret = OB_ITER_END;
  }
  while (OB_SUCC(ret) && count < real_capacity) {
    if (pending_count_ > 0) {
      if (limit.is_valid() && input_row_cnt_ < limit.offset_) {
        const int64_t skipped = MIN(pending_count_, limit.offset_ - input_row_cnt_);
        pending_count_ -= skipped;
        input_row_cnt_ += skipped;
      }
      int64_t delivered = MIN(pending_count_, real_capacity - count);
      if (limit.is_valid()) { delivered = MIN(delivered, limit.limit_ - output_row_cnt_); }
      pending_count_ -= delivered;
      input_row_cnt_ += delivered;
      output_row_cnt_ += delivered;
      count += delivered;
      if (limit.is_valid() && output_row_cnt_ >= limit.limit_) { ret = OB_ITER_END; }
      if (OB_FAIL(ret) || count >= real_capacity) { break; }
    }
    // Refill only at the start of a requested round, never after satisfying
    // capacity/limit. Each token retains its owned snapshot across calls.
    if (refill_mask != 0) {
      refill(0);
      if (dim_count > 1) { refill(1); }
      if (dim_count > 2) { refill(2); }
      refill_mask = 0;
    }
    if (OB_SUCC(ret) && try_window && real_capacity >= 64
        && compare_kind == IdCompareKind::BINARY_STRING && minimum_should_match_ <= 1) {
      int64_t take[MAX_STREAMS] = {};
      int64_t cardinality = 0;
      try_window = false;
      if (count_owned_binary_window(ids, remaining, dim_count, take, cardinality)) {
        // Validation has no side effects. Commit only a fully closed, exact
        // window; pending rows survive the caller's capacity boundary.
        for (int64_t i = 0; i < dim_count; ++i) {
          if (take[i] > 0) {
            ids[i] += take[i];
            remaining[i] -= take[i];
            consumed[i] += take[i];
            heads[i] = remaining[i] > 0 ? &ids[i]->get_datum() : nullptr;
            if (remaining[i] == 0) { refill_mask |= (1U << i); }
          }
        }
        pending_count_ = cardinality;
        try_window = true;
        continue;
      }
    }
    int64_t minimum = -1;
    uint8_t matched_mask = 0;
    int64_t matched_count = 0;
    auto select_head = [&](const int64_t i) {
      if (OB_FAIL(ret) || heads[i] == nullptr) {
      } else if (minimum < 0) {
        minimum = i;
        matched_mask = (1U << i);
        matched_count = 1;
      } else {
        int64_t comparison = 0;
        if (compare_kind == IdCompareKind::GENERIC) {
          ObSRMergeItem left, right;
          left.iter_idx_ = i;
          right.iter_idx_ = minimum;
          ret = merge_cmp_.cmp(left, right, comparison);
        } else {
          const ObDatum &left = *heads[i];
          const ObDatum &right = *heads[minimum];
          if (left.is_null() || right.is_null()) {
            comparison = left.is_null() ? (right.is_null() ? 0 : -1) : 1;
          } else if (compare_kind == IdCompareKind::UNSIGNED_INTEGER) {
            const uint64_t l = left.get_uint64();
            const uint64_t r = right.get_uint64();
            comparison = l < r ? -1 : (l > r ? 1 : 0);
          } else {
            // CS_TYPE_BINARY compares all bytes, including trailing spaces.
            // Preserve byte order for generated document IDs; their payload
            // must not be interpreted as a pair of native integer values.
            const int64_t prefix_length = MIN(left.len_, right.len_);
            // Keep byte ordering while exposing the usual document-ID width
            // to the compiler. Other lengths retain the general comparison.
            if (left.len_ == 16 && right.len_ == 16) {
              comparison = MEMCMP(left.ptr_, right.ptr_, 16);
            } else {
              comparison = prefix_length > 0 ? MEMCMP(left.ptr_, right.ptr_, prefix_length) : 0;
            }
            if (comparison == 0) {
              comparison = left.len_ < right.len_ ? -1 : (left.len_ > right.len_ ? 1 : 0);
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (comparison < 0) {
          minimum = i;
          matched_mask = (1U << i);
          matched_count = 1;
        } else if (comparison == 0) {
          matched_mask |= (1U << i);
          ++matched_count;
        }
      }
    };
    select_head(0);
    if (dim_count > 1) { select_head(1); }
    if (dim_count > 2) { select_head(2); }
    if (OB_FAIL(ret)) {
    } else if (minimum < 0) {
      ret = OB_ITER_END;
    } else {
      if (matched_mask & 1U) { consume(0); }
      if (matched_mask & 2U) { consume(1); }
      if (matched_mask & 4U) { consume(2); }
      // Equivalent to the natural-language inner-product collector: one
      // contribution per matching stream, then matched_cnt >= should_match.
      if (matched_count >= minimum_should_match_) {
        ++input_row_cnt_;
        if (!limit.is_valid() || input_row_cnt_ > limit.offset_) {
          ++count;
          ++output_row_cnt_;
          if (limit.is_valid() && output_row_cnt_ >= limit.limit_) {
            ret = OB_ITER_END;
          }
        }
      }
    }
  }
  for (int64_t i = 0; i < dim_count; ++i) {
    iters[i]->consume_owned_ids(consumed[i]);
  }
  return ret;
}

int ObTextDaaTIter::do_one_merge_round(int64_t &count)
{
  // At most three ordered posting streams need only a linear head selection.
  // Keep the general heap for scoring, Boolean logic, functional lookup and
  // larger queries. The ordinary collector still enforces minimum_should_match.
  if (mode_flag_ != sql::NATURAL_LANGUAGE_MODE || function_lookup_mode_
      || iter_param_->need_project_relevance() || iter_param_->need_filter()
      || dim_iters_->count() > ObSRSimpleMerger::USE_SIMPLE_MERGER_MAX_TABLE_CNT) {
    return ObSRDaaTIterImpl::do_one_merge_round(count);
  }
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < next_round_cnt_; ++i) {
    const int64_t index = next_round_iter_idxes_[i];
    ObISRDaaTDimIter *iter = dim_iters_->at(index);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(iter->get_next_row())) {
      if (ret == OB_ITER_END) {
        iter_domain_ids_[index] = nullptr;
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(iter->get_curr_id(iter_domain_ids_[index]))) {
    }
  }
  next_round_cnt_ = 0;
  int64_t minimum = -1;
  for (int64_t i = 0; OB_SUCC(ret) && i < dim_iters_->count(); ++i) {
    if (iter_domain_ids_[i] == nullptr) {
    } else if (minimum < 0) {
      minimum = i;
      next_round_iter_idxes_[next_round_cnt_++] = i;
    } else {
      ObSRMergeItem left, right;
      left.iter_idx_ = i;
      right.iter_idx_ = minimum;
      int64_t comparison = 0;
      if (OB_FAIL(merge_cmp_.cmp(left, right, comparison))) {
      } else if (comparison < 0) {
        minimum = i;
        next_round_cnt_ = 0;
        next_round_iter_idxes_[next_round_cnt_++] = i;
      } else if (comparison == 0) {
        next_round_iter_idxes_[next_round_cnt_++] = i;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (minimum < 0) {
    ret = OB_ITER_END;
  } else {
    const ObDatum *id = iter_domain_ids_[next_round_iter_idxes_[next_round_cnt_ - 1]];
    for (int64_t i = 0; OB_SUCC(ret) && i < next_round_cnt_; ++i) {
      ret = relevance_collector_->collect_one_dim(next_round_iter_idxes_[i], 0.0);
    }
    double relevance = 0.0;
    bool valid = false;
    if (OB_SUCC(ret) && OB_FAIL(relevance_collector_->get_result(relevance, valid))) {
    } else if (OB_SUCC(ret) && valid) {
      ret = cache_result(count, *id, relevance);
    }
  }
  return ret;
}

ObTextBMWIter::ObTextBMWIter()
  : ObSRBMWIterImpl(),
    bm25_param_estimator_() {}

void ObTextBMWIter::reuse(const bool switch_tablet)
{
  bm25_param_estimator_.reuse(switch_tablet);
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i))->reuse();
    }
  }
  ObSRBMWIterImpl::reuse(switch_tablet);
}

void ObTextBMWIter::reset()
{
  bm25_param_estimator_.reset();
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i))->reset();
    }
  }
  ObSRBMWIterImpl::reset();
}

int ObTextBMWIter::init(const ObTextDaaTParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.base_param_) || OB_ISNULL(param.dim_iters_) || OB_ISNULL(param.allocator_)
      || OB_ISNULL(param.relevance_collector_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObSRBMWIterImpl::init(*param.base_param_, *param.dim_iters_,
                                           *param.allocator_, *param.relevance_collector_))) {
  } else if (OB_FAIL(bm25_param_estimator_.init(param.bm25_param_est_ctx_))) {
  }
  return ret;
}

int ObTextBMWIter::get_next_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (dim_iters_->count() == 0) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(bm25_param_estimator_.do_estimation(*iter_param_->eval_ctx_))) {
  }

  if (FAILEDx(ObSRBMWIterImpl::get_next_rows(capacity, count))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
    }
  }
  return ret;
}

int ObTextBMWIter::init_before_wand_process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!bm25_param_estimator_.is_estimated())) {
    ret = OB_ERR_UNEXPECTED;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < dim_iters_->count(); ++i) {
    ObTextRetrievalBlockMaxIter *block_max_iter = static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i));
    if (OB_FAIL(block_max_iter->init_block_max_iter(
        bm25_param_estimator_.get_total_doc_cnt(), bm25_param_estimator_.get_avg_doc_token_cnt()))) {
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
