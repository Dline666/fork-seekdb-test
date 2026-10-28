"""Capture complete MATCH results and compare candidate with a baseline binary.

Run once per disposable instance with --socket and --output. Pass the first
capture as --compare on the candidate. This does not time or alter benchmarks.
"""
import argparse
import json
import math
import os
import pathlib

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
                                 charset='utf8mb4', read_timeout=60)
    cursor = connection.cursor()
    database = 'fts_equivalence_' + str(os.getpid())
    results = {}

    def query(sql, values=None):
        cursor.execute(sql, values)
        return cursor.fetchall()

    def capture(name, sql, values=None):
        # Every query here must execute successfully on both versions.
        results[name] = query(sql, values)

    try:
        query('create database `' + database + '`')
        query('use `' + database + '`')
        query('create table docs(id int primary key, category int, content text, '
              'fulltext index ft(content) with parser beng)')
        rows = [(1, 1, 'database engine'), (2, 2, 'storage engine'),
                (3, 0, 'database storage'), (4, 1, ''), (5, 2, None),
                (6, 0, 'database database storage'), (7, 1, 'unrelated words'),
                (8, 2, 'Database engine'), (9, 0, 'engine,storage database'),
                (10, 1, 'Café naïve cafe CAFE'), (11, 2, '中文 database'),
                (12, 0, 'term0000 term0001 term0299')]
        rows += [(i, i % 3, ('database ' if i % 2 else '') + ('storage ' if i % 3 else '')
                  + ('engine ' if i % 5 else '') + 'document') for i in range(20, 320)]
        cursor.executemany('insert into docs values(%s,%s,%s)', rows)
        for text in ['database', 'database storage', 'database storage engine',
                     'database database', '', None, 'missingterm', 'the and',
                     '"database storage"', '数据库', 'database 数据库', 'cafe',
                     ' '.join('term%04d' % i for i in range(300))]:
            for hint in ['', '/*+ index(docs ft) */ ', '/*+ no_index(docs ft) */ ']:
                for predicate in ['match(content) against(%s)',
                                  'match(content) against(%s)>0',
                                  '0<match(content) against(%s)',
                                  'match(content) against(%s)+0>0']:
                    capture(repr((text, hint, predicate)), 'select ' + hint
                            + 'id from docs where ' + predicate + ' order by id', (text,))
        for hint in ['', '/*+ index(docs ft) */ ', '/*+ no_index(docs ft) */ ']:
            for mode, text in [('', 'database storage'), (' in boolean mode', '+database -storage')]:
                match = 'match(content) against(%s' + mode + ')'
                for suffix, body in [
                    ('score', 'id,' + match + ' score'),
                    ('shared predicate', 'id,' + match + '>0 present'),
                    ('window', 'id,sum(' + match + ') over() total')]:
                    capture(hint + mode + suffix, 'select ' + hint + body + ' from docs where '
                            + match + '>0 order by id', (text, text))
                capture(hint + mode + 'score order', 'select ' + hint + 'id,' + match
                        + ' score from docs where ' + match + '>0 order by score desc,id limit 17', (text, text))
                capture(hint + mode + 'HAVING', 'select ' + hint + 'id,' + match
                        + ' score from docs where ' + match + '>0 having score>0 order by id', (text, text))
                for threshold in [-1, 0, .0001, 1, 1000, None]:
                    capture(repr((hint, mode, threshold)), 'select ' + hint + 'id from docs where '
                            + match + '>%s order by id', (text, threshold))
        for name, sql in [
            ('group', "select category,count(*) from docs where match(content) against('database') group by category order by category"),
            ('offset', "select id from docs where match(content) against('database storage') order by id limit 17 offset 9"),
            ('two predicates', "select id from docs where match(content) against('database')>0 and match(content) against('engine')>0 order by id"),
            ('OR', "select id from docs where match(content) against('database')>0 or id<10 order by id"),
            ('negation', "select id from docs where not(match(content) against('database')>0) order by id"),
            ('join', "select d.id,e.id from docs d join docs e on e.id=d.id where match(d.content) against('database') order by d.id"),
            ('scalar subquery', "select id,(select count(*) from docs e where e.id=d.id) c from docs d where match(content) against('database')>0 order by id"),
        ]:
            capture(name, sql)
        query("prepare parameter_stmt from 'select id from docs where match(content) against(?)>? order by id'")
        for threshold in [0, 1000, 0, None, -1, .0001]:
            query("set @text='database storage',@threshold=%s", (threshold,))
            capture('prepared ' + repr(threshold), 'execute parameter_stmt using @text,@threshold')
        query('deallocate prepare parameter_stmt')
        query("update docs set content='unrelated replacement' where id=1")
        query('delete from docs where id=2')
        for stage in ['after mutations', 'uncommitted insert', 'after rollback']:
            if stage == 'uncommitted insert':
                query('start transaction')
                query("insert into docs values(1000,0,'database engine')")
            elif stage == 'after rollback':
                query('rollback')
            capture(stage, "select id from docs where match(content) against('database storage engine')>0 order by id")
        # Exercise overlapping dictionary prefixes, both IK modes and multiple
        # indexed columns. Whole row sets and scores are compared with baseline,
        # rather than treating benchmark hit counts as correctness evidence.
        for mode in ['smart', 'max_word']:
            query('create table chinese_docs(id int primary key,title varchar(300),content text,'
                  'fulltext index chinese_ft(title,content) with parser ik '
                  'parser_properties=(ik_mode="' + mode + '"))')
            chinese = [
                (1, '中华人民共和国', '中华人民共和国中央人民政府成立，数据库全文索引检索。'),
                (2, '数据库系统', '数据库数据库 全文检索 OceanBase database engine'),
                (3, '南京市长江大桥', '南京市长江大桥上的研究生命起源项目'),
                (4, '混合 English', '性能优化 tokenizer 中国科学院计算技术研究所'),
                (5, '', ''), (6, None, None),
                (7, '国际化', 'Café naïve Straße Ελληνικά 😀 中文'),
                (8, '长文边界', '中华人民共和国中央人民政府 南京市长江大桥 数据库性能优化。' * 120),
            ]
            cursor.executemany('insert into chinese_docs values(%s,%s,%s)', chinese)
            for text in ['中华人民共和国', '中国 人民', '中央人民政府', '数据库 全文',
                         '南京市 长江大桥', '研究生命起源', 'OceanBase 数据库',
                         '计算技术研究所', '😀', '', None, '不存在的词']:
                for hint in ['', '/*+ index(chinese_docs chinese_ft) */ ',
                             '/*+ no_index(chinese_docs chinese_ft) */ ']:
                    match = 'match(title,content) against(%s)'
                    capture(repr(('IK', mode, text, hint, 'ids')), 'select ' + hint
                            + 'id from chinese_docs where ' + match + '>0 order by id', (text,))
                    capture(repr(('IK', mode, text, hint, 'scores')), 'select ' + hint
                            + 'id,' + match + ' score from chinese_docs where ' + match
                            + '>0 order by score desc,id', (text, text))
            for text in ['中华人民共和国中央人民政府 南京市长江大桥 研究生命起源', chinese[-1][2]]:
                for detail in [False, True]:
                    options = [{'additional_args': [{'ik_mode': mode}]}]
                    if detail:
                        options.append({'output': 'all'})
                    capture(repr(('IK tokens', mode, text, detail)),
                            'select tokenize(%s,%s,%s)', (text, 'ik', json.dumps(options)))
            query("update chinese_docs set content='新的全文检索数据库内容' where id=1")
            query('delete from chinese_docs where id=2')
            capture(mode + ' Chinese mutations', "select id from chinese_docs where "
                    "match(title,content) against('数据库 人民政府')>0 order by id")
            query('drop table chinese_docs')
        result = {'checks': len(results), 'results': results}
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        if args.compare:
            baseline = json.loads(args.compare.read_text())
            actual = json.loads(args.output.read_text())
            differences = [name for name in actual['results']
                           if not equivalent(baseline['results'].get(name), actual['results'][name])]
            if set(baseline['results']) != set(actual['results']) or differences:
                raise AssertionError('MATCH result differences: ' + repr(differences))
        print('PASS full MATCH result capture:', len(results), 'queries;',
              'baseline equivalent' if args.compare else 'baseline recorded', flush=True)
    finally:
        connection.rollback()
        query('drop database if exists `' + database + '`')
        connection.close()


if __name__ == '__main__':
    main()
