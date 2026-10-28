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

#ifndef _OCEANBASE_STORAGE_FTS_IK_OB_IK_CJK_PROCESSOR_H_
#define _OCEANBASE_STORAGE_FTS_IK_OB_IK_CJK_PROCESSOR_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "storage/fts/dict/ob_ft_dict.h"
#include "storage/fts/ik/ob_ik_processor.h"

namespace oceanbase
{
namespace storage
{
class ObFTDictHub;
class ObIKCJKProcessor : public ObIIKProcessor
{
public:
  ObIKCJKProcessor(const ObIFTDict &dict_main, ObIAllocator &alloc)
      : hits_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(alloc, "IKActiveHits")),
        dict_main_(dict_main), cjk_start_(-1), cjk_end_(-1)
  {
  }
  ~ObIKCJKProcessor() override { hits_.reset(); }

public:
  int do_process(TokenizeContext &ctx,
                 const char *ch,
                 const uint8_t char_len,
                 const ObFTCharUtil::CharType type) override;

private:
  // The array interface requires default construction. Keep its copies complete
  // even though ObDATrieHit's cursor-only assignment does not copy dict_.
  struct ActiveHit
  {
    ActiveHit() : hit_(nullptr, 0) {}
    ActiveHit(const ObDATrieHit &hit) : hit_(hit) {}
    ActiveHit(const ActiveHit &hit) : hit_(hit.hit_) {}
    ActiveHit &operator=(const ActiveHit &hit)
    {
      hit_ = hit.hit_;
      hit_.dict_ = hit.hit_.dict_;
      return *this;
    }
    ObDATrieHit hit_;
  };
  common::ObSEArray<ActiveHit, 8> hits_;
  const ObIFTDict &dict_main_;
  int64_t cjk_start_;
  int64_t cjk_end_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIKCJKProcessor);
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_IK_OB_IK_CJK_PROCESSOR_H_
