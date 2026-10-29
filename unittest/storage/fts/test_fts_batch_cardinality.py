"""Capture FTS batch cardinalities on disposable baseline/candidate instances.

Run each binary with --socket and --output; pass the baseline JSON as --compare
on the candidate. This creates and drops its own database and requests native
major compaction. It changes compaction scheduling settings on the instance.
SQL MATCH does not expose minimum_should_match, so explicit MSM thresholds are
not covered here. No benchmark definitions or timing measurements are involved.
"""
import argparse
import json
import os
import pathlib
import time

import pymysql


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--socket', required=True)
    parser.add_argument('--output', required=True, type=pathlib.Path)
    parser.add_argument('--compare', type=pathlib.Path)
    args = parser.parse_args()
    connection = pymysql.connect(unix_socket=args.socket, user='root', autocommit=True,
                                 charset='utf8mb4', read_timeout=180)
    cursor = connection.cursor()
    database = 'fts_batch_cardinality_' + str(os.getpid())
    results = {}
    # Each of the three main postings contains over 12,288 entries: more than
    # three 4,096-row buffers, with alternating overlap and unequal tail sizes.
    row_count = 32791
    specifications = [('ints', 'int', lambda i: i),
                      ('strings', 'varchar(80)', lambda i: 'common-prefix-%08d-tail' % i),
                      ('bytes', 'varbinary(80)',
                       lambda i: b'\0prefix-' + i.to_bytes(4, 'big') + b'-tail\xff')]
    contents = {}

    def query(sql, values=None):
        cursor.execute(sql, values)
        return cursor.fetchall()

    def capture(name, sql, values=None, expected=None):
        try:
            result = query(sql, values)
            if expected is not None:
                assert result == expected, (name, result, expected)
            results[name] = result
        except Exception:
            print('FAILED QUERY', name, sql, repr(values), flush=True)
            raise

    def content(i):
        words = [word for bit, word in enumerate(('alpha', 'bravo', 'charlie'))
                 if i & (1 << bit)]
        if i % 97 == 0:
            words.append('delta')
        if i % 101 == 0:
            words.extend(('alpha', 'alpha'))
        if i % 257 == 0:
            return None
        return ' '.join(words)

    def check_stage(stage):
        for table, _, _ in specifications:
            for text in ('alpha', 'alpha bravo', 'alpha bravo charlie',
                         'alpha alpha bravo', 'alpha bravo missingterm',
                         'missingterm', '', 'alpha bravo charlie delta'):
                terms = set(text.split())
                expected = sum(bool(terms.intersection((value or '').split()))
                               for value in contents[table].values())
                for hint in ('', '/*+ index(%s ft) */ ' % table,
                             '/*+ no_index(%s ft) */ ' % table):
                    name = repr((stage, table, text, hint))
                    capture(name, 'select ' + hint + 'count(*) from ' + table
                            + ' where match(content) against(%s)>0', (text,), ((expected,),))
                # The derived table returns constants, allowing the cardinality
                # path even when OFFSET/LIMIT cuts through posting/output spans.
                if text == 'alpha bravo charlie':
                    for offset, cap in ((0, 0), (0, 1), (1, 17), (255, 257),
                                        (256, 256), (1023, 1025), (4095, 4097),
                                        (12287, 4097), (expected - 1, 7),
                                        (expected, 1), (expected + 1, 257)):
                        capture(repr((stage, table, 'limit', offset, cap)),
                                'select count(*) from (select /*+ index(' + table
                                + ' ft) */ 1 from ' + table
                                + ' where match(content) against(%s)>0 limit %s offset %s)t',
                                (text, cap, offset), ((min(cap, max(0, expected - offset)),),))
            # NULL input, Boolean mode and score consumers must retain fallback
            # behavior; compare deterministic results against the baseline.
            capture(repr((stage, table, 'null search')), 'select count(*) from ' + table
                    + ' where match(content) against(%s)>0', (None,), ((0,),))
            capture(repr((stage, table, 'boolean')), 'select count(*) from ' + table
                    + " where match(content) against('+alpha -bravo' in boolean mode)>0")
            capture(repr((stage, table, 'ids')), 'select hex(id) from ' + table
                    + " where match(content) against('alpha bravo charlie')>0 order by id limit 257")
            capture(repr((stage, table, 'score filter')), 'select count(*) from ' + table
                    + " where match(content) against('alpha bravo charlie')>0.001")
            query("prepare batch_stmt from 'select count(*) from " + table
                  + " where match(content) against(?)>0'")
            for index, text in enumerate(('alpha bravo', 'missingterm', 'alpha bravo charlie',
                                          'alpha bravo', '')):
                query('set @batch_text=%s', (text,))
                capture(repr((stage, table, 'prepared', index)),
                        'execute batch_stmt using @batch_text')
            query('deallocate prepare batch_stmt')

    try:
        query('create database ' + database)
        query('use ' + database)
        for table, key_type, key in specifications:
            query('create table ' + table + '(id ' + key_type
                  + ' primary key,content text,fulltext index ft(content) with parser beng) '
                  + 'row_format=compressed')
            contents[table] = {i: content(i) for i in range(row_count)}
            # Reverse insertion order prevents insertion order from accidentally
            # substituting for the ordered, interleaved posting merge.
            rows = [(key(i), contents[table][i]) for i in reversed(range(row_count))]
            for start in range(0, len(rows), 250):
                cursor.executemany('insert into ' + table + ' values(%s,%s)', rows[start:start + 250])
        check_stage('memtable')
        tablets = query('select tablet_id from oceanbase.__all_tablet_to_table where table_id in '
                        '(select table_id from oceanbase.__all_table where database_id='
                        '(select database_id from oceanbase.__all_database where database_name=%s))',
                        (database,))
        expected_tablets = {row[0] for row in tablets}
        if not expected_tablets:
            raise AssertionError('No data/index tablets found for compaction verification')
        query("alter system set merger_check_interval='10s'")
        query("alter system set ob_compaction_schedule_interval='3s'")
        query('alter system major freeze')
        deadline = time.monotonic() + 180
        while True:
            merged = query("select distinct tablet_id from oceanbase.__all_virtual_tablet_compaction_history "
                           "where type='MAJOR_MERGE' and tablet_id in ("
                           + ','.join(str(tablet) for tablet in sorted(expected_tablets)) + ')')
            if expected_tablets.issubset({row[0] for row in merged}):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError('Native major compaction did not finish for every data/index tablet')
            time.sleep(.5)
        check_stage('major SSTable')
        for table, _, key in specifications:
            for i in (1, 255, 256, 1023, 1024, 4095, 4096, 12287, row_count - 1):
                replacement = 'alpha charlie' if i & 1 else 'bravo delta'
                query('update ' + table + ' set content=%s where id=%s', (replacement, key(i)))
                contents[table][i] = replacement
            for i in (7, 511, 2047, 8191):
                query('delete from ' + table + ' where id=%s', (key(i),))
                del contents[table][i]
            for i in range(row_count, row_count + 1031):
                value = content(i)
                query('insert into ' + table + ' values(%s,%s)', (key(i), value))
                contents[table][i] = value
        check_stage('mixed SSTable and memtable')
        record = {'checks': len(results), 'results': results}
        args.output.write_text(json.dumps(record, ensure_ascii=False, indent=2, default=str) + '\n')
        if args.compare:
            assert json.loads(args.compare.read_text()) == json.loads(args.output.read_text()), \
                'FTS batch cardinality results differ from baseline'
        print('PASS FTS batch cardinality:', len(results), 'query results;',
              'baseline equivalent' if args.compare else 'baseline recorded', flush=True)
    finally:
        connection.rollback()
        query('drop database if exists ' + database)
        connection.close()


if __name__ == '__main__':
    main()
