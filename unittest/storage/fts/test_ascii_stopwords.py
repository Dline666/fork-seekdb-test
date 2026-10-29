"""Compare tokenizer stopword behavior on baseline/candidate SQL instances.

Run with --socket and --output on each disposable instance; add --compare with
the baseline JSON on the candidate. Exercises both parser paths with minimum
token size 1, alternate collations, and simultaneous independent connections.
"""
import argparse
import concurrent.futures
import json
import pathlib
import random
import re
import string

import pymysql


def canonical(value):
    if isinstance(value, list):
        return sorted((canonical(item) for item in value),
                      key=lambda item: json.dumps(item, sort_keys=True, ensure_ascii=False))
    if isinstance(value, dict):
        return {key: canonical(item) for key, item in value.items()}
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--socket', required=True)
    parser.add_argument('--output', required=True, type=pathlib.Path)
    parser.add_argument('--compare', type=pathlib.Path)
    args = parser.parse_args()
    source = (pathlib.Path(__file__).resolve().parents[3]
              / 'src/storage/fts/ob_fts_stop_word.h').read_text()
    declaration = source.split('ob_stop_word_list', 1)[1].split('};', 1)[0]
    stopwords = re.findall(r'"([^"\\]*)"', declaration)
    assert stopwords, 'Could not read authoritative stopword list'
    texts = [None, '', ' ', '\t\r\n']
    for word in stopwords:
        texts.extend((word, word.upper(),
                      ''.join(c.upper() if i % 2 else c for i, c in enumerate(word)),
                      'x' + word, word + 'x', 'x' + word + 'x',
                      word + ' ', word + ' ' * 12, word + '\0', '\0' + word,
                      word + '.', word + '_', word + '1'))
    rng = random.Random(20261029)
    for length in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 17, 31, 63, 83, 85, 129):
        for _ in range(5):
            texts.append(''.join(rng.choice(string.ascii_letters) for _ in range(length)))
    texts.extend(('á à â ä å ã ā', 'ÀBOUT abóut ábout', 'thé thé\0 thé         ',
                  'í ï î ì İ ı', 'é en én èñ', 'a\u0301 a\u0300 the\u0301',
                  'ａ ｔｈｅ ＡＢＯＵＴ ＷＩＴＨ', 'the\u00a0 the\u200b',
                  'a\0b the\0with alpha\0bravo', 'the! a? i, with: from;',
                  '123 _ - . / \\ [ ] { } @ ` ~', '数据库 停用词 😀',
                  'aAaAaAaAa aaaaaaaa aaaaaaaaa abcdefgh ABCDEFGHI',
                  ' '.join(stopwords), ' '.join(word.upper() for word in stopwords),
                  ' '.join(stopwords + ['alpha', 'bravo', 'charlie']) * 12))
    # Retain None while removing duplicate cases (the authoritative list itself
    # contains duplicates). A deterministic order makes JSON captures reusable.
    texts = list(dict.fromkeys(texts))
    options = json.dumps([{'output': 'all'},
                          {'additional_args': [{'min_token_size': 1}, {'max_token_size': 84}]}])

    def connect():
        return pymysql.connect(unix_socket=args.socket, user='root', autocommit=True,
                               charset='utf8mb4', read_timeout=60, write_timeout=60)

    def tokenize(cursor, text, name, collation):
        cursor.execute('select tokenize(convert(%s using utf8mb4) collate '
                       + collation + ',%s,%s)', (text, name, options))
        value = cursor.fetchone()[0]
        return None if value is None else canonical(json.loads(value))

    results = {}
    expected = {}
    connection = connect()
    try:
        cursor = connection.cursor()
        for name in ('beng', 'space'):
            for collation in ('utf8mb4_general_ci', 'utf8mb4_bin', 'utf8mb4_unicode_ci'):
                for index, text in enumerate(texts):
                    value = tokenize(cursor, text, name, collation)
                    expected[name, collation, index] = value
                    results[repr((name, collation, index, text))] = value
    finally:
        connection.close()

    def worker(worker_index):
        connection = connect()
        checks = 0
        try:
            cursor = connection.cursor()
            # Shared checker reads happen concurrently across parser kinds,
            # ASCII hits/misses and Unicode fallback, without shared cursors.
            indices = list(range(0, len(texts), max(1, len(texts) // 32)))
            indices += list(range(max(0, len(texts) - 16), len(texts)))
            for repeat in range(2):
                for index in indices:
                    for name in ('beng', 'space'):
                        collation = ('utf8mb4_general_ci', 'utf8mb4_bin',
                                     'utf8mb4_unicode_ci')[(worker_index + repeat) % 3]
                        value = tokenize(cursor, texts[index], name, collation)
                        assert value == expected[name, collation, index], \
                            (worker_index, repeat, name, collation, index)
                        checks += 1
            return checks
        finally:
            connection.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        concurrent_checks = sum(pool.map(worker, range(6)))
    record = {'checks': len(results), 'concurrent_checks': concurrent_checks,
              'stopwords': stopwords, 'results': results}
    args.output.write_text(json.dumps(record, ensure_ascii=False, indent=2) + '\n')
    if args.compare:
        assert json.loads(args.compare.read_text()) == json.loads(args.output.read_text()), \
            'Stopword tokenizer results differ from baseline'
    print('PASS ASCII stopwords:', len(results), 'serial and', concurrent_checks,
          'concurrent checks;', 'baseline equivalent' if args.compare else 'baseline recorded',
          flush=True)


if __name__ == '__main__':
    main()
