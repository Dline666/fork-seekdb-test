"""Differential and independent-oracle checks for cardinality-only FTS LIMIT.

Use --socket SOCKET --output baseline.json on the baseline, then add
--compare baseline.json on the candidate. Plan texts are saved separately from
result comparisons: candidate eligible plans must remove score/topk, whereas
value-consuming controls must retain scoring. Uses a disposable database.
"""
import argparse
import decimal
import json
import math
import os
import pathlib
import re

import pymysql


def equivalent(left, right):
    if isinstance(left, float) and isinstance(right, (float, int)):
        return math.isclose(left, right, rel_tol=1e-10, abs_tol=1e-12)
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(equivalent(a, b) for a, b in zip(left, right))
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(equivalent(left[key], right[key]) for key in left)
    return left == right


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--socket', required=True)
    parser.add_argument('--output', required=True, type=pathlib.Path)
    parser.add_argument('--compare', type=pathlib.Path)
    args = parser.parse_args()
    connection = pymysql.connect(unix_socket=args.socket, user='root', autocommit=True,
                                 charset='utf8mb4', read_timeout=120)
    cursor = connection.cursor()
    database = 'fts_count_limit_' + str(os.getpid())
    results, plans = {}, {}
    baseline_plans = None
    if args.compare:
        baseline_plan_path = args.compare.with_name(args.compare.stem + '-plans.json')
        baseline_plans = json.loads(baseline_plan_path.read_text())
    # All nonempty words are literal lowercase ASCII words outside stoplists.
    # Python's exact membership oracle is independent of MATCH execution.
    contents = {i: (None if i % 17 == 0 else '' if i % 19 == 0 else
                    ' '.join((['alpha'] * (1 + i % 4) if i % 2 == 0 else [])
                             + (['bravo'] if i % 3 == 0 else [])
                             + (['charlie'] if i % 5 == 0 else []) + ['delta']))
                for i in range(1, 181)}

    def query(sql, values=None):
        cursor.execute(sql, values)
        return cursor.fetchall()

    def capture(name, sql, values=None, expected=None):
        value = query(sql, values)
        if expected is not None:
            assert value == expected, (name, value, expected)
        # Normalize DB-API containers and decimal COUNT projections for JSON.
        results[name] = json.loads(json.dumps(value, default=lambda x:
                                  float(x) if isinstance(x, decimal.Decimal) else str(x)))
        return value

    def oracle(text, limit, offset=0):
        terms = set((text or '').split())
        matched = sum(bool(terms.intersection((content or '').split()))
                      for content in contents.values())
        return ((min(limit, max(0, matched - offset)),),)

    def count_sql(text, limit=20, offset=0, projection='id', order=''):
        return ('select count(*) from (select ' + projection
                + ' from docs where match(content) against(%s)' + order
                + ' limit ' + str(limit) + ' offset ' + str(offset) + ') t', (text,))

    def explain(name, sql, values=None, score=True, ordinary_limit=False):
        rows = query('explain extended ' + sql, values)
        plan = '\n'.join(' '.join(str(cell) for cell in row) for row in rows)
        plans[name] = plan
        if args.compare and not ordinary_limit:
            # Preserve existing score/topk requirements for every ineligible
            # control, even when unrelated rewrites already eliminate a use.
            old = baseline_plans[name]
            assert re.findall(r'calc_relevance=(true|false)', old) == re.findall(
                r'calc_relevance=(true|false)', plan), (name, 'Scoring plan changed', old, plan)
            assert ('with_ties(' in old) == ('with_ties(' in plan), (name, 'Topk plan changed', old, plan)
        # The baseline deliberately scores eligible queries. Only compare-mode
        # validates the changed plan; baseline plans are diagnostic artifacts.
        if args.compare and score is not None:
            expected = 'calc_relevance=' + ('true' if score else 'false')
            assert expected in plan, (name, expected, plan)
            if ordinary_limit:
                assert 'calc_relevance=true' not in plan, (name, plan)
                assert 'with_ties(' not in plan, (name, 'FTS topk annotation remains', plan)
                assert re.search(r'\blimit\b', plan, re.I), (name, 'No ordinary LIMIT', plan)
                assert 'is_index_back=false' in plan and 'is_index_back=true' not in plan, (
                    name, 'Unused projection still causes index lookup', plan)
                assert not re.search(r'\b(?:TABLE|DOMAIN|DOC.?ID)\s+LOOKUP\b', plan, re.I), (
                    name, 'Lookup operator remains', plan)
                # Literal dummy outputs are legitimate. A base-table datum in
                # access/output would still materialize the unused projection.
                materialization = '\n'.join(line for line in plan.splitlines()
                                             if re.search(r'\b(?:output|access)\(', line))
                assert not re.search(r'\bdocs\.', materialization, re.I), (
                    name, 'Base-table output/access remains', plan)

    try:
        query('create database `' + database + '`')
        query('use `' + database + '`')
        query('create table docs(id int primary key, category int, nullable_id int, content text, '
              'fulltext index ft(content) with parser beng)')
        cursor.executemany('insert into docs values(%s,%s,%s,%s)',
                           [(i, i % 3, None if i % 7 == 0 else i, text)
                            for i, text in contents.items()])

        for text in ('alpha', 'alpha bravo', 'charlie', 'missingterm', '', None):
            for limit in (0, 1, 20, 300):
                for offset in (0, 3, 200):
                    sql, values = count_sql(text, limit, offset)
                    capture(repr(('oracle', text, limit, offset)), sql, values,
                            oracle(text, limit, offset))
        for projection in ('id', '1', 'NULL', 'id, category'):
            sql, values = count_sql('alpha bravo', projection=projection)
            capture('eligible projection ' + projection, sql, values, oracle('alpha bravo', 20))
        sql, values = count_sql('alpha bravo')
        explain('eligible', sql, values, score=False, ordinary_limit=True)
        sql, values = count_sql('alpha bravo', offset=3)
        explain('eligible offset', sql, values, score=False, ordinary_limit=True)

        # Controls consume values, have explicit ordering, or have a parent
        # operator that can distinguish which rows survive the child LIMIT.
        controls = {
            'cte count': "with matched as (select id from docs where match(content) against('alpha bravo') limit 20) select count(*) from matched",
            'cte shared values': "with matched as (select id from docs where match(content) against('alpha bravo') limit 20) select (select count(*) from matched),(select sum(id) from matched)",
            'explicit score order': "select count(*) from (select id from docs where match(content) against('alpha bravo') order by match(content) against('alpha bravo') desc limit 20)t",
            'direct ids': "select id from docs where match(content) against('alpha bravo') limit 20",
            'direct scores': "select id,match(content) against('alpha bravo') score from docs where match(content) against('alpha bravo') order by score desc,id limit 20",
            'parent filter': "select count(*) from (select id from docs where match(content) against('alpha bravo') limit 20)t where id<80",
            'parent having': "select count(*) from (select id from docs where match(content) against('alpha bravo') limit 20)t having count(*)>0",
            'parent distinct': "select distinct count(*) from (select id from docs where match(content) against('alpha bravo') limit 20)t",
            'parent group': "select category,count(*) from (select category from docs where match(content) against('alpha bravo') limit 20)t group by category order by category",
            'parent window': "select count(*) over() from (select id from docs where match(content) against('alpha bravo') limit 20)t",
            'parent join': "select count(*) from (select id from docs where match(content) against('alpha bravo') limit 20)t join docs d on d.id=t.id where d.category=1",
            'count column': "select count(nullable_id) from (select nullable_id from docs where match(content) against('alpha bravo') limit 20)t",
            'count score': "select count(score) from (select match(content) against('alpha bravo') score from docs where match(content) against('alpha bravo') limit 20)t",
            'nested values': "select count(*) from (select id from (select id from docs where match(content) against('alpha bravo') limit 20)t where id<80)u",
            'score projection': "select count(*) from (select id,match(content) against('alpha bravo') score from docs where match(content) against('alpha bravo') limit 20)t",
            'expression projection': "select count(*) from (select id+1 x from docs where match(content) against('alpha bravo') limit 20)t",
            'inner distinct': "select count(*) from (select distinct category from docs where match(content) against('alpha bravo') limit 20)t",
            'inner group': "select count(*) from (select category from docs where match(content) against('alpha bravo') group by category limit 20)t",
            'inner window': "select count(*) from (select id,row_number() over(order by id) rn from docs where match(content) against('alpha bravo') limit 20)t",
            'inner having': "select count(*) from (select category from docs where match(content) against('alpha bravo') group by category having count(*)>1 limit 20)t",
        }
        for name, sql in controls.items():
            capture(name, sql)
            # Later projection-pruning/view rewrites can legitimately expose a
            # new pure COUNT relationship. These controls therefore check full
            # results, while these definitely observable-score cases also
            # require scoring in the final physical plan.
            explain(name, sql, score=True if name in (
                'explicit score order', 'direct ids', 'direct scores', 'count score') else None)
        capture('nested count', "select count(*) from (select count(*) n from "
                "(select id from docs where match(content) against('alpha') limit 20)t)u", expected=((1,),))
        capture('inner explicit id order', "select count(*) from (select id from docs where "
                "match(content) against('alpha') order by id limit 20)t", expected=oracle('alpha', 20))
        capture('correlated rescan', "select d.id,(select count(*) from (select id from docs "
                "where match(content) against('alpha bravo') limit 20)t where t.id>d.id) "
                "from docs d where d.id in (1,30,100) order by d.id")

        side_effect_controls = {
            'where assignment': "select count(*) from (select id from docs where "
                "match(content) against('alpha bravo') and (@counter:=@counter+1)>0 limit 20)t",
            'where last insert id': "select count(*) from (select id from docs where "
                "match(content) against('alpha bravo') and last_insert_id(id)>0 limit 20)t",
        }
        for name, sql in side_effect_controls.items():
            query('set @counter=0')
            query('select last_insert_id(0)')
            capture(name, sql)
            capture(name + ' state', 'select @counter,last_insert_id()')
            explain(name, sql, score=None)

        # Probe FOR UPDATE on the baseline too: some supported server modes
        # reject a locking clause within a derived table. Record that explicit
        # capability result, rather than counting a rejected query as coverage.
        locking_sql = ("select count(*) from (select id from docs where "
                       "match(content) against('alpha bravo') limit 20 for update)t")
        query('start transaction')
        try:
            capture('inner for update', locking_sql)
            explain('inner for update', locking_sql, score=None)
        except pymysql.MySQLError as error:
            if error.args[0] not in (1064, 1235):
                raise
            results['inner for update'] = {'unsupported_error_code': error.args[0]}
            print('FOR UPDATE derived-table syntax unsupported:', error.args[0], flush=True)
        finally:
            query('rollback')

        query("prepare count_limit_stmt from 'select count(*) from (select id from docs "
              "where match(content) against(?)>? limit ? offset ?)t'")
        for repeat in range(2):
            for text in ('alpha bravo', 'missingterm', '', None):
                for threshold in (0, 1000, -1, None, 0):
                    for limit, offset in ((0, 0), (1, 0), (20, 3), (300, 0)):
                        query('set @text=%s,@threshold=%s,@lim=%s,@off=%s',
                              (text, threshold, limit, offset))
                        expected = oracle(text, limit, offset) if threshold == 0 else None
                        if threshold is None:
                            expected = ((0,),)
                        capture(repr(('prepared rescan', repeat, text, threshold, limit, offset)),
                                'execute count_limit_stmt using @text,@threshold,@lim,@off', expected=expected)
        query('deallocate prepare count_limit_stmt')

        # Transaction-local membership changes must be reflected in the count;
        # the same prepared structure is executed across rollback boundaries.
        for stage in ('before', 'uncommitted', 'rollback', 'committed'):
            if stage == 'uncommitted':
                query('start transaction')
                query("insert into docs values(1001,0,1001,'alpha bravo')")
                contents[1001] = 'alpha bravo'
                query("update docs set content='alpha' where id=1")
                contents[1] = 'alpha'
                query('delete from docs where id=2')
                contents.pop(2)
            elif stage == 'rollback':
                query('rollback')
                contents.pop(1001)
                contents[1] = 'delta'
                contents[2] = 'alpha alpha alpha delta'
            elif stage == 'committed':
                query("insert into docs values(1002,0,1002,'alpha bravo')")
                contents[1002] = 'alpha bravo'
            for limit in (1, 20, 500):
                sql, values = count_sql('alpha bravo', limit)
                capture(repr(('visibility', stage, limit)), sql, values, oracle('alpha bravo', limit))

        record = {'checks': len(results), 'results': results}
        args.output.write_text(json.dumps(record, ensure_ascii=False, indent=2) + '\n')
        plan_path = args.output.with_name(args.output.stem + '-plans.json')
        plan_path.write_text(json.dumps(plans, ensure_ascii=False, indent=2) + '\n')
        if args.compare:
            assert equivalent(json.loads(args.compare.read_text()), record), 'FTS COUNT/LIMIT results differ'
        print('PASS FTS COUNT/LIMIT:', len(results), 'checks;', len(plans), 'plans;',
              'baseline equivalent and candidate plans verified' if args.compare else 'baseline recorded', flush=True)
    finally:
        try:
            query('rollback')
            query('drop database if exists `' + database + '`')
        finally:
            connection.close()


if __name__ == '__main__':
    main()
