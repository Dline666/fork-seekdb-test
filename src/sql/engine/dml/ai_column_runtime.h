#pragma once

#include "share/ob_thread_pool.h"
#include "sql/engine/dml/ob_dml_ctx_define.h"

namespace oceanbase {
namespace sql {
class ObTableModifyOp;

// Transactional outbox producer, called after a primary-table DAS write.
class AiColumnRuntime
{
public:
  static bool needs_processing(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef);
  static int after_row(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef, ObDmlEventType event);
};

// Owns no business transaction while calling an external model. A durable lease
// and request/definition checks fence both recovery and publication.
class AiColumnWorker : public share::ObThreadPool
{
public:
  AiColumnWorker() : share::ObThreadPool(2) {}
private:
  void run1() override;
};
} // namespace sql
} // namespace oceanbase
