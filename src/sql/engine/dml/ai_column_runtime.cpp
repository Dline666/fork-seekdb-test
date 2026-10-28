#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/dml/ai_column_runtime.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/ob_sql_context.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_server_struct.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "query/session/ob_inner_sql_connection_access.h"
#include "sql/session/ob_local_session_var.h"
#include "lib/thread/ob_thread_name.h"
#include <string>
#include <vector>
#include <cstdlib>
#include <chrono>
#include <condition_variable>
#include <mutex>

namespace oceanbase {
using namespace common;
using namespace observer;
using namespace share::schema;
namespace sql {
namespace {
int64_t ai_fast_dispatch_until = 0;
int64_t ai_dispatch_generation = 0;
std::mutex ai_dispatch_mutex;
std::condition_variable ai_dispatch_condition;
std::string ai_text(const ObString &s) { return std::string(s.ptr() == nullptr ? "" : s.ptr(), s.length()); }
std::string ai_ident(const ObString &s)
{
  std::string out("`");
  for (int64_t i = 0; i < s.length(); ++i) {
    if (s.ptr()[i] == '`') { out += '`'; }
    out += s.ptr()[i];
  }
  return out + '`';
}
// Hex literals are independent of the caller's SQL quoting mode and preserve NULs.
std::string ai_bytes_literal(const std::string &s)
{
  static const char digits[] = "0123456789abcdef";
  std::string out("X'");
  for (unsigned char c : s) { out += digits[c >> 4]; out += digits[c & 15]; }
  return out + "'";
}
std::string ai_literal(const std::string &s)
{
  return "CONVERT(" + ai_bytes_literal(s) + " USING utf8mb4)";
}
std::string ai_request_id()
{
  static int64_t sequence = 0;
  return std::to_string(ObTimeUtility::current_time()) + "-" +
         std::to_string(ATOMIC_AAF(&sequence, 1));
}
const ObTableSchema *ai_get_table(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef)
{
  const ObTableSchema *table = nullptr;
  ObSqlCtx *sql_ctx = op.get_exec_ctx().get_sql_ctx();
  if (sql_ctx != nullptr && sql_ctx->schema_guard_ != nullptr) {
    (void)sql_ctx->schema_guard_->get_table_schema(ctdef.das_base_ctdef_.index_tid_, table);
  }
  return table;
}
int ai_row_value(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef,
                 const ExprFixedArray &row, uint64_t id, std::string &value)
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; i < ctdef.column_ids_.count() && i < row.count(); ++i) {
    if (ctdef.column_ids_.at(i) == id) {
      ObDatum *datum = nullptr;
      ObObj obj;
      ObExpr *expr = row.at(i);
      char *buf = nullptr;
      int64_t len = 0, pos = 0;
      if (OB_FAIL(expr->eval(op.get_eval_ctx(), datum))) {
      } else if (OB_FAIL(datum->to_obj(obj, expr->obj_meta_))) {
      } else if (obj.is_string_type()) {
        // Row keys are replayed by a fresh worker session. Hex bytes preserve
        // quoting, SQL mode and binary/legacy character encodings exactly.
        value = ai_bytes_literal(ai_text(obj.get_string()));
      } else if (obj.get_type() == ObTimestampType) {
        // FROM_UNIXTIME converts the same absolute instant in whichever time
        // zone the replay session uses, unlike a zone-local SQL timestamp text.
        value = "FROM_UNIXTIME(" + std::to_string(obj.get_timestamp()) + " / 1000000.000000)";
      } else if (OB_FAIL(obj.print_sql_literal(buf, len, pos, op.get_exec_ctx().get_allocator(),
                                               op.get_obj_print_params()))) {
      } else {
        value.assign(buf, pos);
      }
      break;
    }
  }
  return ret;
}
int ai_rowkey(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef, const ObTableSchema &table,
              const ExprFixedArray &row, std::string &predicate)
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo &key = table.get_rowkey_info();
  for (int64_t i = 0; OB_SUCC(ret) && i < key.get_size(); ++i) {
    uint64_t id = OB_INVALID_ID;
    const ObColumnSchemaV2 *column = nullptr;
    std::string value;
    if (OB_FAIL(key.get_column_id(i, id))) {
    } else if (OB_ISNULL(column = table.get_column_schema(id))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ai_row_value(op, ctdef, row, id, value))) {
    } else {
      if (!predicate.empty()) { predicate += " AND "; }
      predicate += ai_ident(column->get_column_name_str()) + " = " + value;
    }
  }
  if (OB_SUCC(ret) && (predicate.empty() || predicate.size() > static_cast<size_t>(OB_MAX_VARCHAR_LENGTH))) { ret = OB_SIZE_OVERFLOW; }
  return ret;
}
int ai_dependency_changed(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef,
                           const ObColumnSchemaV2 &column, bool &changed)
{
  int ret = OB_SUCCESS;
  changed = false;
  const std::string deps = ai_text(column.get_ai_dependency_ids());
  const char *p = deps.c_str();
  while (OB_SUCC(ret) && !changed && *p != '\0') {
    char *end = nullptr;
    const uint64_t id = strtoull(p, &end, 10);
    if (end == p) { ret = OB_ERR_UNEXPECTED; break; }
    int64_t index = -1;
    for (int64_t i = 0; i < ctdef.column_ids_.count(); ++i) {
      if (ctdef.column_ids_.at(i) == id) { index = i; break; }
    }
    if (index < 0 || index >= ctdef.old_row_.count() || index >= ctdef.new_row_.count()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      ObExpr &old_expr = *ctdef.old_row_.at(index);
      ObExpr &new_expr = *ctdef.new_row_.at(index);
      ObDatum *old_value = nullptr;
      ObDatum *new_value = nullptr;
      if (OB_FAIL(old_expr.eval(op.get_eval_ctx(), old_value))) {
      } else if (OB_FAIL(new_expr.eval(op.get_eval_ctx(), new_value))) {
      } else if (old_value->is_null() || new_value->is_null()) {
        changed = old_value->is_null() != new_value->is_null();
      } else if (is_lob_storage(old_expr.datum_meta_.type_) && is_lob_storage(new_expr.datum_meta_.type_)) {
        // Compare the stored value through the native LOB comparator. Different
        // locator headers can describe identical LONGTEXT/JSON dependency data.
        int64_t comparison = 0;
        if (OB_FAIL(ObDMLService::check_lob_column_changed(op.get_eval_ctx(),
            old_expr, *old_value, new_expr, *new_value, comparison))) {
        } else { changed = comparison != 0; }
      } else {
        changed = !ObDatum::binary_equal(*old_value, *new_value);
      }
    }
    p = *end == ',' ? end + 1 : end;
  }
  return ret;
}
struct AiTask
{
  int64_t table_id = 0, column_id = 0, version = 0, retry = 0, max_retry = 0, state = 0;
  int64_t is_null = 0;
  std::string id, request, predicate, value;
};
int ai_result_text(sqlclient::ObMySQLResult &result, const char *name, std::string &value)
{
  ObString text;
  int ret = result.get_varchar(name, text);
  if (OB_SUCC(ret)) { value = ai_text(text); }
  return ret;
}
int ai_write(ObISQLClient &client, const std::string &sql, int64_t *count = nullptr)
{
  int64_t affected = 0;
  int ret = client.write(sql.c_str(), affected);
  if (count != nullptr) { *count = affected; }
  return ret;
}
int ai_start_transaction(ObMySQLTransaction &tx)
{
  int ret = tx.start(GCTX.sql_proxy_);
  if (OB_SUCC(ret)) {
    ObSQLSessionInfo *session = query::ObInnerSQLConnectionAccess::get_session(tx.get_connection());
    if (session == nullptr) { ret = OB_ERR_UNEXPECTED; }
    else { session->set_ai_column_internal(true); }
  }
  return ret;
}
int ai_end_transaction(ObMySQLTransaction &tx, bool commit)
{
  ObSQLSessionInfo *session = query::ObInnerSQLConnectionAccess::get_session(tx.get_connection());
  if (session != nullptr) { session->set_ai_column_internal(false); }
  return tx.end(commit);
}
int ai_claim(const std::string &owner, std::vector<AiTask> &tasks)
{
  int ret = OB_SUCCESS;
  std::string request;
  {
    ObMySQLProxy::MySQLResult guard;
    if (OB_FAIL(GCTX.sql_proxy_->read(guard,
        "SELECT request_id FROM oceanbase.__all_ai_column_task "
        "WHERE (state IN (1,3,4,5) AND eligible_ts<=now(6) OR state=2 AND lease_until<now(6)) "
        "AND (lease_until IS NULL OR lease_until<now(6)) ORDER BY eligible_ts,task_id LIMIT 1"))) {
    } else if (OB_FAIL(guard.get_result()->next())) {
    } else { ret = ai_result_text(*guard.get_result(), "request_id", request); }
  }
  if (ret == OB_ITER_END) { return ret; }
  ObMySQLTransaction tx;
  int64_t affected = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ai_start_transaction(tx))) {
  } else if (OB_FAIL(ai_write(tx,
      "UPDATE oceanbase.__all_ai_column_task SET owner_id=" + ai_literal(owner) +
      ",lease_until=DATE_ADD(now(6), INTERVAL 120 SECOND),state=IF(state IN (1,3),2,state) "
      "WHERE request_id=" + ai_literal(request) + " AND (lease_until IS NULL OR lease_until<now(6))", &affected))) {
  } else if (affected == 0) { ret = OB_ITER_END; }
  else {
    ObMySQLProxy::MySQLResult guard;
    std::string query = "SELECT task_id,table_id,column_id,rowkey_predicate,request_id,definition_version,"
      "retry_count,max_retries,state,result_value,result_is_null FROM oceanbase.__all_ai_column_task WHERE request_id=" +
      ai_literal(request) + " AND owner_id=" + ai_literal(owner) + " ORDER BY column_id";
    if (OB_FAIL(tx.read(guard, query.c_str()))) {
    } else {
      sqlclient::ObMySQLResult &r = *guard.get_result();
      while (OB_SUCC(ret) && OB_SUCC(ret = r.next())) {
        AiTask t;
        if (OB_FAIL(ai_result_text(r, "task_id", t.id)) || OB_FAIL(r.get_int("table_id", t.table_id)) ||
            OB_FAIL(r.get_int("column_id", t.column_id)) || OB_FAIL(ai_result_text(r,"rowkey_predicate",t.predicate)) ||
            OB_FAIL(ai_result_text(r,"request_id",t.request)) || OB_FAIL(r.get_int("definition_version",t.version)) ||
            OB_FAIL(r.get_int("retry_count",t.retry)) || OB_FAIL(r.get_int("max_retries",t.max_retry)) ||
            OB_FAIL(r.get_int("state",t.state)) || OB_FAIL(r.get_int("result_is_null",t.is_null))) {
        } else {
          int value_ret = ai_result_text(r,"result_value",t.value);
          if (value_ret != OB_SUCCESS && value_ret != OB_ERR_NULL_VALUE) { ret = value_ret; }
          else { tasks.push_back(t); }
        }
      }
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    }
  }
  if (tx.is_started()) {
    const int end_ret = ai_end_transaction(tx,OB_SUCC(ret));
    if (OB_SUCC(ret)) { ret = end_ret; }
  }
  return ret;
}
int ai_task_target(const AiTask &task, ObSchemaGetterGuard &guard, const ObTableSchema *&table,
                    const ObColumnSchemaV2 *&column, std::string &target)
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema *db = nullptr;
  if (OB_FAIL(GCTX.schema_service_->get_runtime_schema_guard(guard))) {
  } else if (OB_FAIL(guard.get_table_schema(task.table_id, table))) {
  } else if (table == nullptr || (column = table->get_column_schema(task.column_id)) == nullptr ||
             !column->is_ai_column() || column->get_ai_definition_version() != task.version) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (table->get_column_schema(column->get_ai_state_column_id()) == nullptr ||
             table->get_column_schema(column->get_ai_request_column_id()) == nullptr ||
             table->get_column_schema(column->get_ai_result_spec_column_id()) == nullptr) {
    ret = OB_SCHEMA_ERROR;
  } else if (OB_FAIL(guard.get_database_schema(table->get_database_id(), db))) {
  } else if (db == nullptr) { ret = OB_ENTRY_NOT_EXIST; }
  else { target = ai_ident(db->get_database_name_str()) + "." + ai_ident(table->get_table_name_str()); }
  return ret;
}
std::string ai_task_guard(const AiTask &task, const ObTableSchema &table, const ObColumnSchemaV2 &column)
{
  return "(" + task.predicate + ") AND " +
      ai_ident(table.get_column_schema(column.get_ai_request_column_id())->get_column_name_str()) + "=" + ai_literal(task.request) +
      " AND " + ai_ident(table.get_column_schema(column.get_ai_result_spec_column_id())->get_column_name_str()) + "=" +
      ai_literal(std::to_string(task.version));
}
int ai_execute(AiTask &task)
{
  int ret = OB_SUCCESS;
  task.is_null = 0;
  task.value.clear();
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table = nullptr;
  const ObColumnSchemaV2 *column = nullptr;
  std::string target;
  if (OB_FAIL(ai_task_target(task, schema_guard, table, column, target))) { return ret; }
  // This read is outside T1 and outside the lease transaction. The request guard
  // selects only the committed input corresponding to this task.
  std::string query = "SELECT CAST((" + ai_text(column->get_ai_column_expr()) +
      ") AS CHAR) AS ai_value FROM " + target + " WHERE " + ai_task_guard(task,*table,*column);
  ObMySQLTransaction model_read;
  ObArenaAllocator session_var_allocator(ObMemAttr("AIColumnVars"));
  ObLocalSessionVar saved_model_vars(&session_var_allocator);
  bool model_vars_saved = false;
  if (OB_FAIL(ai_start_transaction(model_read))) {
  } else if (OB_FAIL(ObLocalSessionVarHelper::load_session_vars(
      query::ObInnerSQLConnectionAccess::get_session(model_read.get_connection()), saved_model_vars))) {
  } else if (FALSE_IT(model_vars_saved = true)) {
  } else if (OB_FAIL(ObLocalSessionVarHelper::update_session_vars_with_local(
      column->get_local_session_var(),
      *query::ObInnerSQLConnectionAccess::get_session(model_read.get_connection())))) {
  } else {
    ObMySQLProxy::MySQLResult result;
    if (OB_FAIL(model_read.read(result, query.c_str()))) {
    } else if (OB_FAIL(result.get_result()->next())) {
    } else if (OB_FAIL(ai_result_text(*result.get_result(),"ai_value",task.value))) {
      if (ret == OB_ERR_NULL_VALUE) { task.is_null = 1; ret = OB_SUCCESS; }
    }
  }
  if (model_read.is_started()) {
    if (model_vars_saved) {
      const int restore_ret = ObLocalSessionVarHelper::update_session_vars_with_local(
          saved_model_vars,
          *query::ObInnerSQLConnectionAccess::get_session(model_read.get_connection()));
      if (OB_SUCC(ret)) { ret = restore_ret; }
    }
    const int end_ret = ai_end_transaction(model_read,false);
    if (OB_SUCC(ret)) { ret = end_ret; }
  }
  if (OB_SUCC(ret)) {
    // Run the actual column conversion and native index maintenance under a
    // rolled-back transaction before persisting a successful terminal outcome.
    // Bad JSON, overflow, length and vector dimension errors become FAILED.
    ObMySQLTransaction check;
    if (OB_FAIL(ai_start_transaction(check))) {
    } else {
      ret = ai_write(check,"UPDATE " + target + " SET " + ai_ident(column->get_column_name_str()) + "=" +
        (task.is_null ? "NULL" : ai_literal(task.value)) + " WHERE " + ai_task_guard(task,*table,*column));
    }
    if (check.is_started()) {
      const int end_ret = ai_end_transaction(check,false);
      if (OB_SUCC(ret)) { ret = end_ret; }
    }
  }
  return ret;
}
int ai_save_task(AiTask &task, const std::string &owner, bool success)
{
  task.state = success ? 4 : (task.retry < task.max_retry ? 3 : 5);
  if (!success) { ++task.retry; task.value.clear(); task.is_null = 1; }
  int64_t affected = 0;
  int ret = ai_write(*GCTX.sql_proxy_, "UPDATE oceanbase.__all_ai_column_task SET state=" + std::to_string(task.state) +
      ",retry_count=" + std::to_string(task.retry) + ",eligible_ts=DATE_ADD(now(6),INTERVAL 1 SECOND),result_value=" +
      ai_literal(task.value) + ",result_is_null=" + std::to_string(task.is_null) + " WHERE task_id=" + ai_literal(task.id) +
      " AND owner_id=" + ai_literal(owner) + " AND lease_until>now(6)", &affected);
  if (OB_SUCC(ret) && affected != 1) { ret = OB_STATE_NOT_MATCH; }
  return ret;
}
int ai_publish(std::vector<AiTask> &tasks, const std::string &owner)
{
  if (tasks.empty()) { return OB_SUCCESS; }
  int ret = OB_SUCCESS;
  ObMySQLTransaction tx;
  int64_t owned = 0;
  if (OB_FAIL(ai_start_transaction(tx))) {
  } else if (OB_FAIL(ai_write(tx, "UPDATE oceanbase.__all_ai_column_task SET lease_until=DATE_ADD(now(6),INTERVAL 120 SECOND) "
      "WHERE request_id=" + ai_literal(tasks[0].request) + " AND owner_id=" + ai_literal(owner) + " AND lease_until>now(6)", &owned))) {
  } else if (owned != static_cast<int64_t>(tasks.size())) { ret = OB_STATE_NOT_MATCH; }
  else {
    std::string assignments, predicate, target;
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table = nullptr;
    const ObColumnSchemaV2 *column = nullptr;
    for (size_t i = 0; OB_SUCC(ret) && i < tasks.size(); ++i) {
      const AiTask &task = tasks[i];
      schema_guard.reset();
      if (OB_FAIL(ai_task_target(task,schema_guard,table,column,target))) {
        if (ret == OB_ENTRY_NOT_EXIST) {
          // A sibling may have been dropped or redefined while this group ran.
          // Its task is obsolete; surviving siblings still publish atomically.
          ret = OB_SUCCESS;
        }
      } else {
        if (!assignments.empty()) { assignments += ","; predicate += " AND "; }
        assignments += ai_ident(column->get_column_name_str()) + "=" +
            (task.state == 5 || task.is_null ? "NULL" : ai_literal(task.value)) + "," +
            ai_ident(table->get_column_schema(column->get_ai_state_column_id())->get_column_name_str()) +
            (task.state == 5 ? "='FAILED'" : "='READY'");
        predicate += "(" + ai_task_guard(task,*table,*column) + ")";
      }
    }
    if (OB_SUCC(ret) && !assignments.empty()) {
      ret = ai_write(tx,"UPDATE " + target + " SET " + assignments + " WHERE " + predicate);
    }
    if (OB_SUCC(ret)) {
      ret = ai_write(tx,"DELETE FROM oceanbase.__all_ai_column_task WHERE request_id=" +
                         ai_literal(tasks[0].request) + " AND owner_id=" + ai_literal(owner));
    }
  }
  if (tx.is_started()) {
    const int end_ret = ai_end_transaction(tx,OB_SUCC(ret));
    if (OB_SUCC(ret)) { ret = end_ret; }
  }
  return ret;
}
} // namespace

bool AiColumnRuntime::needs_processing(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef)
{
  const ObSQLSessionInfo *session = op.get_exec_ctx().get_my_session();
  return ctdef.is_primary_index_ && ctdef.has_ai_columns_ &&
      (!session->is_ai_column_internal() || session->get_ai_column_retry_id() != OB_INVALID_ID);
}

int AiColumnRuntime::after_row(ObTableModifyOp &op, const ObDMLBaseCtDef &ctdef, ObDmlEventType event)
{
  int ret = OB_SUCCESS;
  if (!needs_processing(op,ctdef)) { return ret; }
  const ObTableSchema *table = ai_get_table(op,ctdef);
  const ObDatabaseSchema *db = nullptr;
  std::string old_key, new_key;
  const bool inserting = event == ObDmlEventType::DE_INSERTING;
  const bool deleting = event == ObDmlEventType::DE_DELETING;
  if (!inserting && OB_FAIL(ai_rowkey(op,ctdef,*table,ctdef.old_row_,old_key))) {
  } else if (!deleting && OB_FAIL(ai_rowkey(op,ctdef,*table,ctdef.new_row_,new_key))) {
  } else if (OB_FAIL(op.get_exec_ctx().get_sql_ctx()->schema_guard_->get_database_schema(table->get_database_id(),db))) {
  } else if (db == nullptr) { ret = OB_ERR_UNEXPECTED; }
  if (OB_FAIL(ret)) {
    LOG_WARN("AI column row key preparation failed", K(ret), K(event), KP(table));
    return ret;
  }
  const std::string table_id = std::to_string(table->get_table_id());
  const std::string request = ai_request_id();
  const std::string target = ai_ident(db->get_database_name_str()) + "." + ai_ident(table->get_table_name_str());
  const uint64_t forced_column_id = op.get_exec_ctx().get_my_session()->get_ai_column_retry_id();
  std::string assignments;
  std::vector<std::string> writes;
  for (auto it = table->column_begin(); OB_SUCC(ret) && it != table->column_end(); ++it) {
    const ObColumnSchemaV2 &column = **it;
    if (!column.is_ai_column()) { continue; }
    if (forced_column_id != OB_INVALID_ID && column.get_column_id() != forced_column_id) { continue; }
    if (forced_column_id == OB_INVALID_ID && !deleting && !inserting && old_key == new_key) {
      bool changed = false;
      if (OB_FAIL(ai_dependency_changed(op,ctdef,column,changed))) { break; }
      if (!changed) { continue; }
    }
    const std::string col_id = std::to_string(column.get_column_id());
    writes.push_back("DELETE FROM oceanbase.__all_ai_column_task WHERE table_id=" + table_id +
      " AND column_id=" + col_id + " AND rowkey_predicate=" + ai_literal(inserting ? new_key : old_key));
    if (deleting) { continue; }
    const ObColumnSchemaV2 *state = table->get_column_schema(column.get_ai_state_column_id());
    const ObColumnSchemaV2 *req = table->get_column_schema(column.get_ai_request_column_id());
    const ObColumnSchemaV2 *spec = table->get_column_schema(column.get_ai_result_spec_column_id());
    if (state == nullptr || req == nullptr || spec == nullptr) { ret = OB_ERR_UNEXPECTED; break; }
    if (!assignments.empty()) { assignments += ","; }
    assignments += ai_ident(column.get_column_name_str()) + "=NULL," + ai_ident(state->get_column_name_str()) +
      "='PENDING'," + ai_ident(req->get_column_name_str()) + "=" + ai_literal(request) + "," +
      ai_ident(spec->get_column_name_str()) + "=" + ai_literal(std::to_string(column.get_ai_definition_version()));
    writes.push_back("INSERT INTO oceanbase.__all_ai_column_task(task_id,table_id,column_id,rowkey_predicate,request_id,"
      "definition_version,eligible_ts,retry_count,max_retries,state,owner_id,lease_until,result_value,result_is_null) VALUES(" +
      ai_literal(request+"-"+col_id) + "," + table_id + "," + col_id + "," + ai_literal(new_key) + "," + ai_literal(request) +
      "," + std::to_string(column.get_ai_definition_version()) + ",now(6),0," + std::to_string(column.get_ai_max_retries()) +
      ",1,NULL,NULL,NULL,0)");
  }
  if (OB_SUCC(ret) && !writes.empty()) {
    ObSQLSessionInfo *session = op.get_exec_ctx().get_my_session();
    const bool saved = session->is_ai_column_internal();
    const uint64_t saved_retry_id = session->get_ai_column_retry_id();
    if (OB_FAIL(op.open_inner_conn())) {
      LOG_WARN("AI column failed to open inner connection", K(ret));
    } else if (OB_FAIL(op.begin_nested_session(true))) {
      LOG_WARN("AI column failed to begin nested session", K(ret));
    } else {
      session->set_ai_column_internal(true);
      // Publication updates the hidden state and result through nested DML;
      // only the outer RETRY write may force a fresh outbox entry.
      session->set_ai_column_retry_id(OB_INVALID_ID);
      if (!assignments.empty()) {
        ret = op.execute_write(("UPDATE " + target + " SET " + assignments + " WHERE " + new_key).c_str());
        if (OB_FAIL(ret)) { LOG_WARN("AI column invalidation update failed", K(ret)); }
      }
      for (const std::string &sql : writes) {
        if (OB_SUCC(ret)) {
          ret = op.execute_write(sql.c_str());
          if (OB_FAIL(ret)) { LOG_WARN("AI column task outbox write failed", K(ret)); }
        }
      }
      const int end_ret = op.end_nested_session();
      session->set_ai_column_internal(saved);
      session->set_ai_column_retry_id(saved_retry_id);
      if (OB_SUCC(ret)) { ret = end_ret; }
      if (OB_SUCC(ret)) {
        ATOMIC_STORE(&ai_fast_dispatch_until, ObTimeUtility::current_time() + 2000000);
        {
          std::lock_guard<std::mutex> guard(ai_dispatch_mutex);
          ATOMIC_AAF(&ai_dispatch_generation, 1);
        }
        ai_dispatch_condition.notify_all();
      }
    }
  }
  return ret;
}

void AiColumnWorker::run1()
{
  lib::set_thread_name("AIColumnWorker");
  const std::string owner = ai_request_id() + "-" + std::to_string(get_thread_idx());
  while (!has_set_stop()) {
    const int64_t dispatch_generation = ATOMIC_LOAD(&ai_dispatch_generation);
    std::vector<AiTask> tasks;
    int ret = ai_claim(owner,tasks);
    if (OB_SUCC(ret) && !tasks.empty()) {
      bool terminal = true;
      for (AiTask &task : tasks) {
        if (has_set_stop()) { break; }
        if (task.state != 4 && task.state != 5) {
          int64_t affected = 0;
          ret = ai_write(*GCTX.sql_proxy_,"UPDATE oceanbase.__all_ai_column_task SET lease_until=DATE_ADD(now(6),INTERVAL 120 SECOND) "
            "WHERE request_id=" + ai_literal(task.request) + " AND owner_id=" + ai_literal(owner) + " AND lease_until>now(6)",&affected);
          if (OB_FAIL(ret) || affected != static_cast<int64_t>(tasks.size())) { ret = OB_STATE_NOT_MATCH; break; }
          const int execute_ret = ai_execute(task);
          if (OB_FAIL(ai_save_task(task,owner,execute_ret == OB_SUCCESS))) { break; }
        }
        terminal = terminal && (task.state == 4 || task.state == 5);
      }
      if (OB_SUCC(ret) && terminal && !has_set_stop()) { ret = ai_publish(tasks,owner); }
      // Relinquish all surviving rows even after a stale request or SQL failure.
      // On process interruption the same rows become reclaimable at lease expiry.
      (void)ai_write(*GCTX.sql_proxy_,"UPDATE oceanbase.__all_ai_column_task SET lease_until=NULL,owner_id=NULL,"
        "state=IF(state=2,1,state) WHERE owner_id=" + ai_literal(owner));
    }
    if (ret != OB_SUCCESS && ret != OB_ITER_END && REACH_TIME_INTERVAL(1000000)) {
      LOG_WARN("AI column worker will retry persisted work", K(ret));
    }
    // Poll promptly after a producer signals new work. Idle databases retain
    // the lower scan rate so the worker does not tax unrelated SQL workloads.
    const bool active = !tasks.empty() ||
        ObTimeUtility::current_time() < ATOMIC_LOAD(&ai_fast_dispatch_until);
    std::unique_lock<std::mutex> dispatch_guard(ai_dispatch_mutex);
    ai_dispatch_condition.wait_for(dispatch_guard, std::chrono::microseconds(active ? 20000 : 100000),
        [dispatch_generation] { return ATOMIC_LOAD(&ai_dispatch_generation) != dispatch_generation; });
  }
}
} // namespace sql
} // namespace oceanbase
