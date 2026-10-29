"""Check string bytes across native compaction and mixed writes.

The inputs encourage fixed, non-hex string-diff encoding; SQL has no force-codec
interface, so this test does not claim that a particular codec was selected.
Use only on a disposable instance: it changes compaction scheduling settings.
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
    parser.add_argument('--output', type=pathlib.Path)
    args = parser.parse_args()
    connection = pymysql.connect(unix_socket=args.socket, user='root', autocommit=True,
                                 charset='utf8mb4', read_timeout=180, write_timeout=180)
    cursor = connection.cursor()
    database = 'string_diff_batches_' + str(os.getpid())
    lengths = (16, 32, 128, 129, 256, 512)
    segment_lengths = (1, 2, 4, 8, 16, 3, 7, 1, 2, 4, 8, 16)
    fixed_columns = [('b' + str(n), n, False) for n in lengths]
    fixed_columns += [('v' + str(n), n, True) for n in lengths]
    names = [name for name, _, _ in fixed_columns] + ['hex_friendly', 'variable_bytes']
    row_count = 4099
    rows = {}
    checks = 0

    def query(sql, values=None):
        cursor.execute(sql, values)
        return cursor.fetchall()

    def make_row(index, revision=0):
        values = []
        for column, (_, length, varchar) in enumerate(fixed_columns):
            segment = segment_lengths[column]
            raw = bytearray((b'K' if varchar else b'\xa5') * length)
            start = (length - segment) // 2
            for offset in range(segment):
                raw[start + offset] = (33 + (index * 17 + offset * 31 + revision * 13) % 94
                                       if varchar else
                                       (index * 37 + offset * 53 + revision * 19) % 256)
            # Every non-NULL value in each column has exactly the target byte
            # length, including after updates. No empty strings in these fields.
            value = bytes(raw)
            if index % 13 == 0 or index % (17 + 2 * column) == 0:
                value = None
            values.append(value)
        # Separate columns exercise hex-friendly and genuinely variable paths.
        hexadecimal = ('ab%028x7f' % (index + revision)).encode('ascii')
        variable = b'variable-prefix-' + bytes([index % 256]) * (index % 53)
        if index % 19 == 0:
            variable = b''
        if index % 13 == 0:
            hexadecimal = variable = None
        return tuple(values + [hexadecimal, variable])

    def insert(indices, revision=0):
        batch = []
        for index in indices:
            rows[index] = make_row(index, revision)
            # VARCHAR receives ASCII strings, while its oracle remains bytes.
            values = [value.decode('ascii') if 6 <= column < 12 and value is not None else value
                      for column, value in enumerate(rows[index])]
            batch.append((index, *values))
            if len(batch) == 200:
                cursor.executemany('insert into docs values(' + ','.join(['%s'] * 15) + ')', batch)
                batch = []
        if batch:
            cursor.executemany('insert into docs values(' + ','.join(['%s'] * 15) + ')', batch)

    def check(stage, hint, where='', predicate=lambda index: True, reverse=False, cap=None):
        nonlocal checks
        selected = [index for index in sorted(rows, reverse=reverse) if predicate(index)]
        if cap is not None:
            selected = selected[:cap]
        expected = tuple((index, *(None if value is None else value.hex().upper()
                                   for value in rows[index])) for index in selected)
        sql = 'select ' + hint + 'id,' + ','.join('hex(' + name + ')' for name in names)
        sql += ' from docs' + (' where ' + where if where else '')
        sql += ' order by id ' + ('desc' if reverse else 'asc')
        if cap is not None:
            sql += ' limit ' + str(cap)
        actual = query(sql)
        assert actual == expected, (stage, hint, where, reverse, cap, len(actual), len(expected))
        checks += 1

    def check_stage(stage):
        for hint in ('/*+ no_use_column_store(docs) */ ', '/*+ use_column_store(docs) */ '):
            check(stage, hint)
            check(stage, hint, reverse=True)
            for cap in (1, 255, 256, 257):
                check(stage, hint, cap=cap)
            for lower, upper in ((1, 258), (255, 512), (1023, 1280)):
                check(stage, hint, 'id>=%d and id<%d' % (lower, upper),
                      lambda index, low=lower, high=upper: low <= index < high,
                      reverse=lower == 255)
            check(stage, hint, 'mod(id,7) in (0,3)', lambda index: index % 7 in (0, 3))
            check(stage, hint, 'mod(id,11) in (1,4,9)', lambda index: index % 11 in (1, 4, 9),
                  reverse=True, cap=257)
            check(stage, hint, 'mod(id,13)=0', lambda index: index % 13 == 0)
            check(stage, hint, 'id<0', lambda index: False)

    compactions = []
    try:
        query('create database ' + database)
        query('use ' + database)
        definitions = ['id int primary key']
        for name, length, varchar in fixed_columns:
            definitions.append(name + (' varchar(%d) character set utf8mb4 collate utf8mb4_bin'
                                        if varchar else ' varbinary(%d)') % length)
        definitions += ['hex_friendly varbinary(32)', 'variable_bytes varbinary(100)']
        query('create table docs(' + ','.join(definitions) + ') row_format=compressed')
        insert(reversed(range(row_count)))
        check_stage('memtable')
        table_id = query('select table_id from oceanbase.__all_table where database_id='
                         '(select database_id from oceanbase.__all_database where database_name=%s) '
                         "and table_name='docs'", (database,))[0][0]
        tablets = {row[0] for row in query('select tablet_id from oceanbase.__all_tablet_to_table '
                                         'where table_id=%s', (table_id,))}
        assert tablets, 'No native tablets found'
        query("alter system set merger_check_interval='10s'")
        query("alter system set ob_compaction_schedule_interval='3s'")
        query('alter system major freeze')
        deadline = time.monotonic() + 180
        while True:
            compactions = query("select distinct tablet_id from oceanbase.__all_virtual_tablet_compaction_history "
                                "where type='MAJOR_MERGE' and tablet_id in("
                                + ','.join(str(tablet) for tablet in sorted(tablets)) + ')')
            if tablets.issubset({row[0] for row in compactions}):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError('Native major compaction did not complete')
            time.sleep(.5)
        check_stage('major SSTable')
        for index in (1, 13, 17, 255, 256, 257, 1023, 1024, 4095, 4098):
            rows[index] = make_row(index, revision=1)
            values = [value.decode('ascii') if 6 <= column < 12 and value is not None else value
                      for column, value in enumerate(rows[index])]
            query('update docs set ' + ','.join(name + '=%s' for name in names) + ' where id=%s',
                  (*values, index))
        for index in (7, 511, 2047):
            query('delete from docs where id=%s', (index,))
            del rows[index]
        insert(range(row_count, row_count + 263), revision=1)
        check_stage('mixed SSTable and memtable')
        record = {'passed': True, 'checks': checks, 'rows': len(rows),
                  'native_major_tablets': sorted(row[0] for row in compactions),
                  'codec_selection': 'Not forced or asserted; inputs encourage fixed non-hex encoding.'}
        if args.output:
            args.output.write_text(json.dumps(record, indent=2) + '\n')
        print('PASS string diff batches:', checks, 'byte-oracle queries;', len(rows), 'rows', flush=True)
    finally:
        connection.rollback()
        query('drop database if exists ' + database)
        connection.close()


if __name__ == '__main__':
    main()
