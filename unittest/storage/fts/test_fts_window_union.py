"""Differential checks for exact count-only posting windows and buffered tails.

Run with --socket/--output, then --compare baseline.json on the candidate.
SQL LIMIT values exercise externally observable boundaries; they do not force
an internal iterator capacity. Existing batch-cardinality coverage supplies the
larger generated-sequence rollover cases. SQL does not expose MSM directly.
"""
import argparse
import json
import os
import pathlib

import pymysql


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--socket', required=True)
    parser.add_argument('--output', required=True, type=pathlib.Path)
    parser.add_argument('--compare', type=pathlib.Path)
    args = parser.parse_args()
    connection = pymysql.connect(unix_socket=args.socket, user='root', autocommit=True,
                                 charset='utf8mb4', read_timeout=120)
    cursor = connection.cursor()
    database = 'fts_window_union_' + str(os.getpid())
    results = {}
    contents = {i: ' '.join((["alpha"] if i % 2 else [])
                           + (["bravo"] if i % 3 else [])
                           + (["charlie"] if i % 5 else []) + ['delta'])
                for i in range(1, 1808)}
    # Different key representations plus multiple physical tablets exercise
    # dispatch/fallback and lifetime boundaries without depending on doc ID bits.
    tables = ('generated_ids', 'unsigned_ids', 'heap_ids', 'partitioned')

    def query(sql, values=None):
        cursor.execute(sql, values)
        return cursor.fetchall()

    def capture(name, sql, values=None, expected=None):
        actual = query(sql, values)
        if expected is not None:
            assert actual == expected, (name, actual, expected)
        results[name] = actual

    def expected(text, limit=None, offset=0):
        terms = set((text or '').split())
        total = sum(bool(terms.intersection((value or '').split()))
                    for value in contents.values())
        total = max(0, total - offset)
        return ((total if limit is None else min(total, limit),),)

    def check(stage):
        for table in tables:
            for text in ('alpha', 'alpha bravo', 'alpha bravo charlie',
                         'alpha alpha bravo', 'bravo charlie', 'missingterm', '', None):
                capture(repr((stage, table, text, 'all')),
                        'select count(*) from ' + table + ' where match(content) against(%s)',
                        (text,), expected(text))
                for limit in (1, 63, 64, 65, 127, 128, 129, 256, 257):
                    for offset in (0, 511, 1700, 3000):
                        capture(repr((stage, table, text, limit, offset)),
                                'select count(*) from (select id from ' + table
                                + ' where match(content) against(%s) limit %s offset %s)t',
                                (text, limit, offset), expected(text, limit, offset))
            # Score/ID consumers must remain unchanged too.
            capture(stage + table + ' ranked', 'select id from ' + table
                    + " where match(content) against('alpha bravo') order by "
                    + "match(content) against('alpha bravo') desc,id limit 65")

    try:
        query('create database `' + database + '`')
        query('use `' + database + '`')
        for table in tables:
            key_type = 'bigint unsigned' if table in ('unsigned_ids', 'heap_ids') else 'varchar(40)'
            suffix = ' partition by hash(id) partitions 3' if table == 'partitioned' else ''
            # HASH requires a numeric key on all supported versions.
            if table == 'partitioned':
                key_type = 'bigint'
            primary_key = '' if table == 'heap_ids' else ' primary key'
            query('create table ' + table + '(id ' + key_type
                  + primary_key + ',content text,fulltext index ft(content) with parser beng)' + suffix)
            rows = [(i if table != 'generated_ids' else 'row-%08d' % i, value)
                    for i, value in contents.items()]
            cursor.executemany('insert into ' + table + ' values(%s,%s)', rows)
        check('initial')
        for table in tables:
            query("prepare win_stmt from 'select count(*) from (select id from " + table
                  + " where match(content) against(?) limit ? offset ?)t'")
            for repeat in range(3):
                for text, limit, offset in (('alpha bravo charlie', 129, 63),
                                           ('missingterm', 64, 0), (None, 65, 0),
                                           ('alpha', 257, 127), ('bravo charlie', 1, 511)):
                    query('set @q=%s,@n=%s,@o=%s', (text, limit, offset))
                    capture(repr(('prepared', table, repeat, text, limit, offset)),
                            'execute win_stmt using @q,@n,@o', expected=expected(text, limit, offset))
            query('deallocate prepare win_stmt')
        query('start transaction')
        for table in tables:
            query('delete from ' + table + ' where id=%s',
                  ('row-00000001' if table == 'generated_ids' else 1,))
            query('insert into ' + table + ' values(%s,%s)',
                  ('row-00002000' if table == 'generated_ids' else 2000, 'alpha bravo charlie'))
        old = contents.pop(1)
        contents[2000] = 'alpha bravo charlie'
        check('uncommitted')
        query('rollback')
        contents.pop(2000)
        contents[1] = old
        check('rollback')
        record = {'checks': len(results), 'results': results}
        args.output.write_text(json.dumps(record, ensure_ascii=False, indent=2) + '\n')
        if args.compare:
            assert json.loads(args.compare.read_text()) == json.loads(args.output.read_text()), 'Window union results differ'
        print('PASS exact-window union:', len(results), 'checks;',
              'baseline equivalent' if args.compare else 'baseline recorded', flush=True)
    finally:
        try:
            query('rollback')
            query('drop database if exists `' + database + '`')
        finally:
            connection.close()


if __name__ == '__main__':
    main()
