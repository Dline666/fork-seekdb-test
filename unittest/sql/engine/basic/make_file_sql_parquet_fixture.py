"""Small deterministic Parquet v1 fixture writer for native reader regressions.

Uses the Apache Parquet Thrift field numbers and Compact Protocol. It emits flat
required/optional columns, PLAIN or dictionary encoding, raw Snappy literals,
and three row groups. It performs no SQL and needs only Python's stdlib.
"""
import datetime
import itertools
import struct

I32, I64, BINARY, LIST, STRUCT = 5, 6, 8, 9, 12


def unsigned(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def encode(kind, value):
    if kind in (I32, I64):
        return unsigned((value << 1) ^ (value >> 63))
    if kind == BINARY:
        value = value.encode() if isinstance(value, str) else value
        return unsigned(len(value)) + value
    if kind == LIST:
        item_kind, items = value
        length = len(items)
        prefix = bytes([(min(length, 15) << 4) | item_kind])
        return prefix + (unsigned(length) if length >= 15 else b'') + b''.join(encode(item_kind, item) for item in items)
    if kind == STRUCT:
        previous = 0
        output = bytearray()
        for field, field_kind, item in sorted(value):
            delta = field - previous
            output += bytes([(delta << 4) | field_kind]) if 0 < delta <= 15 else bytes([field_kind]) + encode(I32, field)
            output += encode(field_kind, item)
            previous = field
        return bytes(output) + b'\0'
    raise ValueError(kind)


def rle(values):
    output = bytearray()
    for value, group in itertools.groupby(values):
        count = sum(1 for _ in group)
        output += unsigned(count << 1) + bytes([value])
    return bytes(output)


def snappy_literal(value):
    if not value:
        return b'\0'
    length = len(value) - 1
    if length < 60:
        tag = bytes([length << 2])
    else:
        size = max(1, (length.bit_length() + 7) // 8)
        tag = bytes([(59 + size) << 2]) + length.to_bytes(size, 'little')
    return unsigned(len(value)) + tag + value


def plain(physical, values):
    if physical == 0:
        output = bytearray((len(values) + 7) // 8)
        for index, value in enumerate(values):
            output[index // 8] |= int(value) << (index % 8)
        return bytes(output)
    if physical in (1, 2, 4, 5):
        fmt = {1: '<i', 2: '<q', 4: '<f', 5: '<d'}[physical]
        return b''.join(struct.pack(fmt, value) for value in values)
    return b''.join(struct.pack('<I', len(value.encode())) + value.encode() for value in values)


def rows():
    values = []
    start = datetime.date(2026, 10, 1)
    for index in range(25):
        values.append({'id': index, 'qty': index - 12,
            'score': None if 8 <= index < 16 else index / 4,
            'ratio': None if index % 5 == 0 else index / 8,
            'name': None if index % 4 == 0 else ['数据库', 'alpha', 'comma,quote"\nline'][index % 3],
            'flag': None if index % 6 == 0 else index % 2 == 0,
            'day': None if index % 7 == 0 else start + datetime.timedelta(days=index),
            'created': None if index % 7 == 0 else datetime.datetime(2026, 10, 1, 12, 13, 14, index * 1001)})
    return values


def write_fixture(path, dictionary=False, snappy=False, corrupt_column=None):
    # (name, physical enum, optional, converted enum)
    columns = [('id', 2, False, None), ('qty', 1, False, None),
        ('score', 5, True, None), ('ratio', 4, True, None), ('name', 6, True, 0),
        ('flag', 0, True, None), ('day', 1, True, 6), ('created', 2, True, 10)]
    schema = [[(4, BINARY, 'schema'), (5, I32, len(columns))]]
    for name, physical, optional, converted in columns:
        element = [(1, I32, physical), (3, I32, int(optional)), (4, BINARY, name)]
        if converted is not None:
            element.append((6, I32, converted))
        schema.append(element)
    output = bytearray(b'PAR1')
    row_groups = []
    all_rows = rows()
    for begin in (0, 8, 16):
        group = all_rows[begin:begin + 8] if begin < 16 else all_rows[16:]
        chunks = []
        uncompressed_total = compressed_total = 0
        for name, physical, optional, _ in columns:
            values = [row[name] for row in group]
            present = [value for value in values if value is not None]
            if name == 'day':
                present = [(value - datetime.date(1970, 1, 1)).days for value in present]
            if name == 'created':
                present = [(value - datetime.datetime(1970, 1, 1)).days * 86400000000 +
                           (value - datetime.datetime(1970, 1, 1)).seconds * 1000000 + value.microsecond
                           for value in present]
            levels = rle([int(value is not None) for value in values]) if optional else b''
            definition = struct.pack('<I', len(levels)) + levels if optional else b''
            dictionary_offset = None
            uncompressed = compressed = 0
            use_dictionary = dictionary and name == 'name' and present
            if use_dictionary:
                unique = list(dict.fromkeys(present))
                body = plain(physical, unique)
                packed = snappy_literal(body) if snappy else body
                header = encode(STRUCT, [(1, I32, 2), (2, I32, len(body)), (3, I32, len(packed)),
                    (7, STRUCT, [(1, I32, len(unique)), (2, I32, 0)])])
                dictionary_offset = len(output)
                output += header + packed
                uncompressed += len(header) + len(body)
                compressed += len(header) + len(packed)
                encoded_values = bytes([max(1, (len(unique) - 1).bit_length())]) + rle([unique.index(value) for value in present])
            else:
                encoded_values = plain(physical, present)
            body = definition + encoded_values
            packed = snappy_literal(body) if snappy else body
            data_offset = len(output)
            header = encode(STRUCT, [(1, I32, 0), (2, I32, len(body)), (3, I32, len(packed)),
                (5, STRUCT, [(1, I32, len(values)), (2, I32, 99 if name == corrupt_column else (8 if use_dictionary else 0)), (3, I32, 3), (4, I32, 3)])])
            output += header + packed
            uncompressed += len(header) + len(body)
            compressed += len(header) + len(packed)
            metadata = [(1, I32, physical), (2, LIST, (I32, [0, 3, 8] if use_dictionary else [0, 3])),
                (3, LIST, (BINARY, [name])), (4, I32, int(snappy)), (5, I64, len(values)),
                (6, I64, uncompressed), (7, I64, compressed), (9, I64, data_offset)]
            if dictionary_offset is not None:
                metadata.append((11, I64, dictionary_offset))
            chunks.append([(2, I64, dictionary_offset if dictionary_offset is not None else data_offset), (3, STRUCT, metadata)])
            uncompressed_total += uncompressed
            compressed_total += compressed
        row_groups.append([(1, LIST, (STRUCT, chunks)), (2, I64, uncompressed_total),
                           (3, I64, len(group)), (6, I64, compressed_total)])
    footer = encode(STRUCT, [(1, I32, 1), (2, LIST, (STRUCT, schema)), (3, I64, len(all_rows)),
                            (4, LIST, (STRUCT, row_groups)), (6, BINARY, 'seekdb deterministic test fixture')])
    output += footer + struct.pack('<I', len(footer)) + b'PAR1'
    path.write_bytes(output)
