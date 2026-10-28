"""Native Parquet SQL type/row-group/encoding/pruning regressions, stdlib fixtures."""
import argparse
import json
import pathlib

import pymysql
from make_file_sql_parquet_fixture import rows, write_fixture


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--socket', required=True)
    parser.add_argument('--fixtures', required=True, type=pathlib.Path)
    args = parser.parse_args()
    connection = pymysql.connect(unix_socket=args.socket, user='root', autocommit=True,
                                 charset='utf8mb4', read_timeout=30)
    passed = failed = 0
    cursor = connection.cursor()
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
        except Exception as error:
            failed += 1
            print('FAIL', name, type(error).__name__, str(error), flush=True)
    def error(sql):
        try:
            query(sql)
        except pymysql.MySQLError:
            return
        raise AssertionError('expected malformed Parquet error')
    expected_rows = rows()
    names = ('id', 'qty', 'score', 'ratio', 'name', 'flag', 'day', 'created')
    try:
        query('SET SESSION enable_file_sql=ON')
        paths = []
        for dictionary in (False, True):
            for snappy in (False, True):
                label = ('dictionary' if dictionary else 'plain') + ('-snappy' if snappy else '-uncompressed')
                path = args.fixtures / ('multiple-' + label + '.parquet')
                write_fixture(path, dictionary=dictionary, snappy=snappy)
                source = 'read_parquet(' + connection.escape(str(path)) + ')'
                paths.append(source)
                expected = tuple(tuple(row[name] for name in names) for row in expected_rows)
                check(label + ' all types, NULLs and three row groups', lambda source=source, expected=expected:
                      equal(query('SELECT ' + ','.join(names) + ' FROM ' + source + ' ORDER BY id'), expected))
                check(label + ' aggregate across row groups', lambda source=source:
                      equal(query('SELECT COUNT(*),COUNT(score),SUM(id),SUM(qty) FROM ' + source), ((25, 17, 300, 0),)))
                check(label + ' projected filter limit across boundaries', lambda source=source:
                      equal(query('SELECT id,name FROM ' + source + ' WHERE id BETWEEN 7 AND 18 ORDER BY id DESC LIMIT 5'),
                            tuple((row['id'], row['name']) for row in reversed(expected_rows[14:19]))))
                def describe(source=source):
                    actual = query('DESCRIBE SELECT * FROM ' + source)
                    equal(tuple((row[0], row[1]) for row in actual), tuple(zip(names,
                          ('BIGINT','BIGINT','DOUBLE','DOUBLE','VARCHAR','BOOLEAN','DATE','DATETIME'))))
                check(label + ' footer SQL schema', describe)
        source = paths[-1]
        check('Parquet self join reads both scans', lambda:
              equal(query('SELECT COUNT(*),SUM(a.qty) FROM ' + source + ' a JOIN ' + source + ' b ON a.id=b.id'), ((25, 0),)))
        csv = args.fixtures / 'parquet-join.csv'
        csv.write_text('id,team\n0,zero\n8,eight\n24,last\n', encoding='utf-8')
        csv_source = 'read_csv(' + connection.escape(str(csv)) + ')'
        check('Parquet LEFT JOIN CSV including unmatched rows', lambda:
              equal(query('SELECT p.id,c.team FROM ' + source + ' p LEFT JOIN ' + csv_source +
                          ' c ON p.id=c.id WHERE p.id IN(0,1,8,24) ORDER BY p.id'),
                    ((0,'zero'),(1,None),(8,'eight'),(24,'last'))))
        corrupt = args.fixtures / 'pruned-malformed-column.parquet'
        write_fixture(corrupt, snappy=True, corrupt_column='score')
        corrupt_source = 'read_parquet(' + connection.escape(str(corrupt)) + ')'
        check('column pruning skips malformed unreferenced data page', lambda:
              equal(query('SELECT COUNT(id),SUM(qty) FROM ' + corrupt_source), ((25, 0),)))
        check('referenced malformed data page is rejected', lambda: error('SELECT score FROM ' + corrupt_source))
        for name, payload in [('truncated.parquet', b'PAR1broken'), ('bad-footer.parquet', b'PAR1' + bytes(64) + b'PAR1')]:
            path = args.fixtures / name
            path.write_bytes(payload)
            check(name + ' fails without harming SQL', lambda path=path:
                  error('SELECT * FROM read_parquet(' + connection.escape(str(path)) + ')'))
        check('ordinary SQL remains usable', lambda: equal(query('SELECT 1'), ((1,),)))
        print('SUMMARY', json.dumps({'passed':passed,'failed':failed}), flush=True)
    finally:
        connection.close()
    raise SystemExit(bool(failed))


if __name__ == '__main__':
    main()
