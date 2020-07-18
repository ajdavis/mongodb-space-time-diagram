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

HostID: /(d|s|m)\d+\|/;

JsonOrText: Json | Text;

Json: json=/\{.*\}$/;

Text: text=/.*$/;

OtherLine: message=/.*$/;
"""


def process_timestamp(obj):
    obj.timestamp = datetime.datetime(
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


@singledispatch
def process(line):
    print(f'process({line})')


@process.register
def process_jstest_line(line: mm['JSTestLine']):
    print(f'process_jstest_line({line})')
    if line.message.__class__.__name__ == 'Json':
        print(line.message.parsed)


for l in model.lines:
    process(l)
