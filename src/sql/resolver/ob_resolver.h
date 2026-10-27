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

#ifndef OCEANBASE_SQL_RESOLVER_OB_RESOLVER
#define OCEANBASE_SQL_RESOLVER_OB_RESOLVER

#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;

/// the interface of this module
class ObResolver
{
public:
  enum IsPrepared
  {
    IS_PREPARED_STMT,
    IS_NOT_PREPARED_STMT
  };

  explicit ObResolver(ObResolverParams &params);
  virtual ~ObResolver();

  virtual int resolve(IsPrepared if_prepared, const ParseNode &parse_tree, ObStmt *&stmt);
  ObResolverParams &get_params() { return params_; }

private:
  template <typename ResolverType>
  int stmt_resolver_func(ObResolverParams &params, const ParseNode &parse_tree, ObStmt *&stmt);

  template <typename SelectResolverType>
  int select_stmt_resolver_func(ObResolverParams &params, const ParseNode &parse_tree, ObStmt *&stmt);

  // DuckDB style DESCRIBE: "DESCRIBE SELECT * FROM read_*()" reports the
  // inferred file schema instead of an EXPLAIN plan. Returns a synthesized
  // SELECT over a static schema table, or leaves describe_select NULL when the
  // statement is a plain EXPLAIN or the describe target is not a file table.
  int build_describe_select(const ParseNode &parse_tree, ParseNode *&describe_select);
private:
  // data members
  ObResolverParams params_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_RESOLVER_OB_RESOLVER */
