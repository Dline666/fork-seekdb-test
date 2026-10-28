"""Compare typed TOKENIZE defaults with the validated JSON configuration path.

Requires PyMySQL and an already running disposable seekdb instance. This test
never starts a server and does not change the official benchmark workload.
"""
import argparse
import json
import uuid

import pymysql


def canonical(value):
    if isinstance(value, list):
        return sorted((canonical(item) for item in value),
                      key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True))
    if isinstance(value, dict):
        return {key: canonical(item) for key, item in value.items()}
    return value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--socket', required=True)
    args = parser.parse_args()
    connection = pymysql.connect(unix_socket=args.socket, user='root', autocommit=True,
                                 charset='utf8mb4', read_timeout=30)
    checks = 0
    database = 'tokenizer_defaults_' + uuid.uuid4().hex
    database_created = False
    try:
        cursor = connection.cursor()

        def outcome(sql, values):
            try:
                cursor.execute(sql, values)
                result = cursor.fetchone()[0]
                return 'value', canonical(json.loads(result)) if result is not None else None
            except pymysql.MySQLError as error:
                return 'error', error.args

        def compare(text, name, collation='utf8mb4_general_ci', options='[]'):
            nonlocal checks
            prefix = 'SELECT TOKENIZE(CONVERT(%s USING utf8mb4) COLLATE ' + collation + ',%s'
            actual = outcome(prefix + ')', (text, name))
            expected = outcome(prefix + ',%s)', (text, name, options))
            if actual != expected:
                raise AssertionError((text, name, collation, options, actual, expected))
            checks += 1

        texts = [None, '', ' ', 'a b c', 'Hello WORLD', 'repeat repeat REPEAT',
                 '数据库 English 123 核心分词', 'Café naïve Straße İSTANBUL Ελληνικά 😀',
                 'ABC\0DEF', 'line\nnext\tword\r\nlast', 'éabcé xyz', 'foo_bar foo-bar a.b:c',
                 'a' * 100]
        for name in ['space', 'beng', 'ngram', 'ngram2', 'ik']:
            for text in texts:
                for collation in ['utf8mb4_general_ci', 'utf8mb4_bin', 'utf8mb4_unicode_ci']:
                    compare(text, name, collation)
            for alias in [name.upper(), '  ' + name.upper() + '  ']:
                compare('数据库 Database PERFORMANCE repeated repeated', alias)

        # Explicit defaults pass the same typed values through JSON validation.
        explicit = {
            'space': [{'min_token_size': 3}, {'max_token_size': 84}],
            'beng': [{'min_token_size': 3}, {'max_token_size': 84}],
            'ngram': [{'ngram_token_size': 2}],
            'ngram2': [{'min_ngram_size': 2}, {'max_ngram_size': 3}],
            'ik': [{'ik_mode': 'smart'}],
        }
        for name, additional in explicit.items():
            options = json.dumps([{'additional_args': additional}])
            compare('数据库 Database words WORDS repeated repeated 0123456789', name, options=options)

        for name in [None, '', 'missing_parser', 'beng.1', 'ik.1', 'x' * 100]:
            compare('invalid parser still validates with empty or NULL input', name)
            compare(None, name)

        # Parser and input values change on every row. Neither may be retained
        # from a previous row while the two-argument expression is evaluated.
        cursor.execute('CREATE DATABASE `' + database + '`')
        database_created = True
        cursor.execute('USE `' + database + '`')
        cursor.execute('CREATE TABLE tokenizer_default_rows '
                       '(id INT PRIMARY KEY, content VARCHAR(500), parser_name VARCHAR(40))')
        values = [(index, text, ['space', 'beng', 'ngram', 'ngram2', 'ik'][index % 5])
                  for index, text in enumerate(texts * 2)]
        cursor.executemany('INSERT INTO tokenizer_default_rows VALUES(%s,%s,%s)', values)
        cursor.execute('SELECT id,TOKENIZE(content,parser_name),'
                       'TOKENIZE(content,parser_name,JSON_ARRAY()) '
                       'FROM tokenizer_default_rows ORDER BY id DESC')
        for row_id, actual, expected in cursor.fetchall():
            if canonical(json.loads(actual)) != canonical(json.loads(expected)):
                raise AssertionError(('dynamic parser', row_id, actual, expected))
            checks += 1
        print('PASS TOKENIZE typed defaults:', checks, 'equivalence checks')
    finally:
        try:
            if database_created:
                connection.cursor().execute('DROP DATABASE `' + database + '`')
        finally:
            connection.close()


if __name__ == '__main__':
    main()
