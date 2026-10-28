#pragma once

namespace oceanbase {
namespace sql {
// A scheduling hint only. The durable outbox remains the source of work.
void notify_ai_column_work_committed();
} // namespace sql
} // namespace oceanbase
