"""Capture full IK output at dictionary-derived range and first-character boundaries.

Run --socket SOCKET --output baseline.json, then add --compare baseline.json
on the candidate. Reads authoritative dictionary arrays and the range size;
does not inject malformed/overlapping ranges or exercise their fallback path.
"""
import argparse
import hashlib
import itertools
import json
import pathlib
import re

import pymysql


def read_words(source, array_name):
    match = re.search(r'static const char \*' + array_name + r'\[\]\s*=\s*\{(.*?)\};',
                      source, re.S)
    assert match, 'Missing dictionary array: ' + array_name
    literals = re.findall(r'"(?:[^"\\]|\\.)*"', match.group(1))
    words = [json.loads(literal) for literal in literals]
    assert words and all(words), 'Empty dictionary or dictionary word'
    return words


def partition_ranges(words, limit):
    # Mirror build_ranges_concurrently_thread_pool: the limit-th word fixes
    # the final first character, and the next different first character starts
    # the next range without being consumed by the previous one.
    start = 0
    while start < len(words):
        end = min(start + limit, len(words))
        if end < len(words):
            final_character = words[end - 1][0]
            while end < len(words) and words[end][0] == final_character:
                end += 1
        yield start, end
        start = end


def make_cases(root):
    source = (root / 'src/storage/fts/dict/ob_ik_dic.cpp').read_text()
    header = (root / 'src/storage/fts/dict/ob_ft_range_dict.h').read_text()
    limit = int(re.search(r'DEFAULT_KEY_PER_RANGE\s*=\s*(\d+)', header).group(1))
    texts = {}
    provenance = {}

    def add(text, origin):
        texts.setdefault(text, []).append(origin)

    for name in ('main_dic', 'quan_dict', 'stop_dic'):
        words = read_words(source, name)
        ranges = []
        selected_words = set()
        boundary_points = set()
        for number, (start, end) in enumerate(partition_ranges(words, limit)):
            chunk = words[start:end]
            # DAT boundaries are min/max root characters, even if the raw
            # source were ever to stop being sorted within a range.
            first_characters = [word[0] for word in chunk]
            low, high = min(first_characters), max(first_characters)
            ranges.append({'start_index': start, 'end_index_exclusive': end,
                           'first_word': chunk[0], 'last_word': chunk[-1],
                           'minimum_first_character': low, 'maximum_first_character': high})
            for word in (chunk[0], chunk[-1]):
                selected_words.add(word)
                add(word, f'{name}:range:{number}:endpoint-word')
            for character in (low, high):
                for delta in (-1, 0, 1):
                    point = ord(character) + delta
                    if 0 <= point <= 0x10ffff and not 0xd800 <= point <= 0xdfff:
                        boundary_points.add(point)
                        nearby = chr(point)
                        add(nearby, f'{name}:range:{number}:root-neighbor:{delta}')
                        add(nearby + chunk[0][1:] + ' ' + chunk[-1] + nearby,
                            f'{name}:range:{number}:root-neighbor-context:{delta}')
            add(chunk[0] + ' ' + chunk[-1] + ' ' + chunk[0] + chunk[-1],
                f'{name}:range:{number}:endpoint-combination')

        groups = [(character, list(group)) for character, group in
                  itertools.groupby(words, key=lambda word: word[0])]
        # Uniform deterministic samples include both ends of the real source
        # group list, without issuing queries for thousands of first chars.
        count = min(64, len(groups))
        indices = sorted({i * (len(groups) - 1) // max(1, count - 1)
                          for i in range(count)})
        sampled_groups = []
        for index in indices:
            character, group = groups[index]
            sampled_groups.append({'group_index': index, 'first_character': character,
                                   'first_word': group[0], 'last_word': group[-1]})
            for word in (group[0], group[-1]):
                selected_words.add(word)
                add(word, f'{name}:first-character-group:{index}:endpoint-word')
                if name == 'quan_dict':
                    add('三' + word + ' 12' + word + ' 一百二十' + word,
                        f'{name}:first-character-group:{index}:quantifier-context')
        provenance[name] = {'source_word_count': len(words), 'ranges': ranges,
                            'first_character_group_count': len(groups),
                            'sampled_groups': sampled_groups,
                            'selected_source_word_count': len(selected_words),
                            'range_neighbor_codepoint_count': len(boundary_points)}
    return texts, {'range_word_limit': limit, 'dictionaries': provenance}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--socket', required=True)
    parser.add_argument('--output', required=True, type=pathlib.Path)
    parser.add_argument('--compare', type=pathlib.Path)
    args = parser.parse_args()
    root = pathlib.Path(__file__).resolve().parents[3]
    cases, provenance = make_cases(root)
    results = {}
    connection = pymysql.connect(unix_socket=args.socket, user='root', autocommit=True,
                                 charset='utf8mb4', read_timeout=60)
    try:
        with connection.cursor() as cursor:
            for mode in ('smart', 'max_word'):
                options = json.dumps([{'additional_args': [{'ik_mode': mode}]}, {'output': 'all'}])
                for collation in ('utf8mb4_general_ci', 'utf8mb4_bin', 'utf8mb4_unicode_ci'):
                    outputs = []
                    for text in cases:
                        cursor.execute('select tokenize(convert(%s using utf8mb4) collate '
                                       + collation + ',%s,%s)', (text, 'ik', options))
                        value = cursor.fetchone()[0]
                        # Preserve every output field and list order, not just
                        # token sets or counts.
                        outputs.append(None if value is None else json.loads(value))
                    results[mode + ':' + collation] = outputs
    finally:
        connection.close()
    record = {'checks': len(cases) * len(results), 'case_count': len(cases),
              'case_sha256': hashlib.sha256(json.dumps(list(cases), ensure_ascii=False).encode()).hexdigest(),
              'provenance': provenance, 'cases': cases, 'results': results}
    args.output.write_text(json.dumps(record, ensure_ascii=False, indent=2) + '\n')
    if args.compare:
        assert json.loads(args.compare.read_text()) == record, 'IK dictionary boundary output differs'
    print('PASS IK dictionary boundaries:', record['checks'], 'queries;', len(cases),
          'distinct inputs;', 'baseline equivalent' if args.compare else 'baseline recorded', flush=True)
    for name, source in provenance['dictionaries'].items():
        print(name, 'source words:', source['source_word_count'], 'ranges:', len(source['ranges']),
              'sampled groups:', len(source['sampled_groups']),
              'selected words:', source['selected_source_word_count'],
              'boundary codepoints:', source['range_neighbor_codepoint_count'], flush=True)


if __name__ == '__main__':
    main()
