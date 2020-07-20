import datetime
import sys
from functools import singledispatch

from textx import metamodel_from_str
import ujson

grammar = r"""
Model: lines+=Line;

Line: JSTestLine | OtherLine;

JSTestLine:
    '[js_test:' js_test_name=ID '] ' timestamp=Timestamp
    hostID=HostID? message=JsonOrText
;

Timestamp:
    year=/\d{4}/ '-' month=/\d{2}/ '-' day=/\d{2}/ 'T'
    hour=/\d{2}/ ':' minute=/\d{2}/ ':' second=/\d{2}/ '.' millis=/\d{3}/ 
    '+0000'
;

HostID: hostType=/d|s|m/ port=INT '|';

JsonOrText: Json | Text;

Json: json=/\{.*\}$/;

Text: text=/.*$/;

OtherLine: message=/.*$/;
"""


def process_timestamp(obj):
    return datetime.datetime(
        int(obj.year), int(obj.month), int(obj.day), int(obj.hour),
        int(obj.minute), int(obj.second), 1000 * int(obj.millis))


def process_json(obj):
    obj.parsed = ujson.loads(obj.json)


obj_processors = {
    'Timestamp': process_timestamp,
    'Json': process_json
}

mm = metamodel_from_str(grammar)

mm.register_obj_processors(obj_processors)
model = mm.model_from_file(sys.argv[1])


def dict_get(dct, *path):
    for p in path:
        if p in dct:
            dct = dct[p]
        else:
            return None

    return dct


@singledispatch
def process(line, lineno):
    raise ValueError(f'Cannot process line {lineno}: {line}')


# https://github.com/DistributedClocks/shiviz/wiki
# ShiViz parses the log using a user-specified regular expression. The regular
# expression must contain three capture groups:
#
# event: The event message
# host: The host / process for the event
# clock: The vector clock, in JSON {"host": timestamp} format. The local host
# must be represented in the vector clock (i.e. the host specified in the host
# capture groups must be one of the hosts in the vector clock).
@process.register
def process_jstest_line(line: mm['JSTestLine'], lineno):
    print(f'process_jstest_line({line})')
    if line.message.__class__.__name__ == 'Text':
        print(f'{line.timestamp} {line.message}')
        return

    msg_id = line.message.parsed['id']
    # 'Sending heartbeat'.
    if msg_id == 4615670:
        source = line.message.parsed['attr']['heartbeatObj']['from']
        # TODO: Why is source sometimes empty?
        if not source:
            return

        source_port = int(source.split(':')[1])
        assert source_port == line.hostID.port
        target_port = line.message.parsed['attr']['target'].split(':')[1]
        print(f'{line.timestamp} Send HB'
              f' from {source_port} to {target_port}')
    # 'Received heartbeat request'.
    elif msg_id == 24095:
        source = line.message.parsed['attr']['from']
        if not source:
            return

        source_port = source.split(':')[1]
        print(f'{line.timestamp} Receive HB'
              f' from {source_port} to {line.hostID.port}')
    # 'Generated heartbeat response'.
    elif msg_id == 24097:
        source = line.message.parsed['attr']['from']
        if not source:
            return

        source_port = source.split(':')[1]
        print(f'{line.timestamp} Reply to HB'
              f' from {source_port} to {line.hostID.port}')
    # 'Received response to heartbeat'.
    elif msg_id == 4615620:
        # Yes, the heartbeat reply's source is called 'target'.
        source = line.message.parsed['attr']['target']
        source_port = source.split(':')[1]
        print(f'{line.timestamp} Receive HB reply'
              f' from {source_port} to {line.hostID.port}')


@process.register
def process_other_line(line: mm['OtherLine'], lineno):
    print(line.message)


for i, line in enumerate(model.lines, start=1):
    try:
        process(line, i)
    except Exception as exc:
        print(f'{exc!r} parsing line {i}: {line}')
