"""Compare natural-language existence retrieval with the native scored path.

Run against an explicitly selected disposable Unix-socket instance. This test
does not modify benchmark data or compute a performance score.
"""
import argparse
import os

import pymysql


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--socket', required=True)
    args = parser.parse_args()
    connection = pymysql.connect(unix_socket=args.socket, user='root', password='',
        autocommit=True, charset='utf8mb4', read_timeout=30, write_timeout=30)
    cursor = connection.cursor()
    passed = 0
    failed = 0
    database = 'fts_existence_' + str(os.getpid())

    def query(sql, params=None):
        cursor.execute(sql, params)
        return cursor.fetchall()

    def equal(actual, expected):
        if actual != expected:
            raise AssertionError((actual, expected))

    def check(name, function):
        nonlocal passed, failed
        try:
            function()
            passed += 1
            print('PASS', name, flush=True)
        except Exception as exception:
            failed += 1
            print('FAIL', name, type(exception).__name__, str(exception), flush=True)

    def explain(sql, params=None):
        return '\n'.join(str(field) for row in query('explain ' + sql, params) for field in row)

    def compare(text):
        # Adding zero requires the numeric MATCH value and keeps the original
        # BM25 path. Both sides retain the same snapshot and inverted index.
        equal(query('select /*+ index(docs ft) */ id from docs where match(content) against(%s)>0 order by id', (text,)),
              query('select /*+ index(docs ft) */ id from docs where match(content) against(%s)+0>0 order by id', (text,)))

    try:
        query('create database `' + database + '`')
        query('use `' + database + '`')
        query('create table docs(id int primary key, content text, '
              'fulltext index ft(content) with parser beng)')
        rows = [(1, 'database engine'), (2, 'storage engine'), (3, 'database storage'),
                (4, ''), (5, None), (6, 'database database storage'), (7, 'unrelated words'),
                (8, 'Database engine'), (9, 'engine,storage database'),
                (10, 'databases engines'), (11, 'quoted "database storage"'),
                (12, '中文 database')]
        rows += [(i, ('database ' if i % 2 else '') + ('storage ' if i % 3 else '')
                  + ('engine ' if i % 5 else '') + 'document') for i in range(20, 220)]
        cursor.executemany('insert into docs values(%s,%s)', rows)

        def existence_plan():
            plan = explain("select count(*) from docs where match(content) against('database')")
            if 'calc_relevance=false' not in plan:
                raise AssertionError(plan)
        check('plain MATCH predicate uses existence retrieval', existence_plan)
        for text in ['database', 'database storage', 'database storage engine',
                     'database database', '', None, 'missingterm', 'the and',
                     '"database storage"', '数据库', 'database 数据库']:
            check('scored path equivalence ' + repr(text), lambda text=text: compare(text))

        check('greater than zero predicate', lambda: equal(
            query("select count(*) from docs where match(content) against('database storage')>0"),
            query("select count(*) from docs where match(content) against('database storage')+0>0")))
        check('zero less than MATCH predicate', lambda: equal(
            query("select count(*) from docs where 0<match(content) against('database storage')"),
            query("select count(*) from docs where 0<match(content) against('database storage')+0")))
        check('bounded subquery count', lambda: equal(
            query("select count(*) from(select id from docs where match(content) against('database storage') limit 20) d"),
            ((20,),)))

        def scored_plan(sql):
            # A small fixture can use functional lookup even with an INDEX
            # hint. Both it and scored retrieval are native score producers;
            # neither may be replaced by the ID-only existence path.
            indexed = sql.replace('select ', 'select /*+ index(docs ft) */ ', 1)
            plan = explain(indexed)
            if ('calc_relevance=false' in plan or not (
                    'calc_relevance=true' in plan or 'has_functional_lookup=true' in plan)):
                raise AssertionError(plan)
            fallback = sql.replace('select ', 'select /*+ no_index(docs ft) */ ', 1)
            equal(query(indexed), query(fallback))
        for name, sql in [
            ('projected score', "select id,match(content) against('database') score from docs where match(content) against('database')>0 order by id"),
            ('projected boolean shared expression', "select id,match(content) against('database')>0 present from docs where match(content) against('database')>0 order by id"),
            ('score ordering', "select id from docs where match(content) against('database')>0 order by match(content) against('database') desc,id limit 5"),
            ('nonzero threshold', "select id from docs where match(content) against('database')>0.1 order by id"),
            ('HAVING score', "select id,match(content) against('database') score from docs where match(content) against('database')>0 having score>0 order by id"),
            ('window score', "select id,sum(match(content) against('database')) over() total from docs where match(content) against('database')>0 order by id"),
            ('BOOLEAN mode', "select id from docs where match(content) against('+database -storage' in boolean mode)>0 order by id"),
        ]:
            check(name + ' preserves scoring', lambda sql=sql: scored_plan(sql))

        def parameters():
            query("prepare existence_stmt from 'select count(*) from docs where match(content) against(?) > ?'")
            query("set @text='database',@threshold=0")
            first = query('execute existence_stmt using @text,@threshold')
            equal(first, query("select count(*) from docs where match(content) against('database')+0>0"))
            query('set @threshold=1000')
            equal(query('execute existence_stmt using @text,@threshold'), ((0,),))
            query('set @threshold=0')
            equal(query('execute existence_stmt using @text,@threshold'), first)
            query('deallocate prepare existence_stmt')
        check('prepared thresholds retain plan-cache constraints', parameters)

        def plain_access():
            equal(query("select /*+ no_index(docs ft) */ id from docs where match(content) against('database storage')>0 order by id"),
                  query("select id from docs where match(content) against('database storage')+0>0 order by id"))
        check('plain access fallback equivalence', plain_access)

        def mutations():
            query("update docs set content='unrelated replacement' where id=1")
            query('delete from docs where id=2')
            compare('database storage engine')
            query('start transaction')
            query("insert into docs values(1000,'database engine')")
            compare('database engine')
            query('rollback')
            compare('database engine')
        check('updates deletes and transaction rollback', mutations)

        def repeatable_read():
            other = pymysql.connect(unix_socket=args.socket, user='root', password='',
                database=database, autocommit=True, charset='utf8mb4')
            try:
                query('set session transaction isolation level repeatable read')
                query('start transaction with consistent snapshot')
                before = query("select count(*) from docs where match(content) against('database')>0")
                with other.cursor() as writer:
                    writer.execute("insert into docs values(1001,'database engine')")
                equal(query("select count(*) from docs where match(content) against('database')>0"), before)
                compare('database')
                query('commit')
            finally:
                other.close()
        check('repeatable read snapshot is preserved', repeatable_read)

        def many_tokens():
            text = ' '.join('term%04d' % i for i in range(300))
            query("insert into docs values(1002,'term0000 term0001 term0299')")
            compare(text)
        check('more than 256 terms TAAT deduplication', many_tokens)
        print('SUMMARY', {'passed': passed, 'failed': failed}, flush=True)
    finally:
        connection.rollback()
        query('drop database if exists `' + database + '`')
        connection.close()
    raise SystemExit(bool(failed))


if __name__ == '__main__':
    main()
