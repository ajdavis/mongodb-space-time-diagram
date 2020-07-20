import datetime
import logging
import sys
from functools import singledispatch

from textx import metamodel_from_str
import ujson

logging.basicConfig()

grammar = r"""
Model: lines+=Line;

Line: JSTestLine | OtherLine;

JSTestLine:
    '[js_test:' js_test_name=ID '] ' timestamp=Timestamp
    hostID=HostID? log_msg=JsonOrText
;

Timestamp:
    year=/\d{4}/ '-' month=/\d{2}/ '-' day=/\d{2}/ 'T'
    hour=/\d{2}/ ':' minute=/\d{2}/ ':' second=/\d{2}/ '.' millis=/\d{3}/ 
    /[+-]\d{4}/
;

HostID: hostType=/d|s|m/ port=INT '|';

JsonOrText: Json | Text;

Json: json=/\{.*\}$/;

Text: text=/.*$/;

OtherLine: message=/.*$/;
"""

mm = metamodel_from_str(grammar)
mm.register_obj_processors({
    'Timestamp': lambda obj: datetime.datetime(
        int(obj.year), int(obj.month), int(obj.day), int(obj.hour),
        int(obj.minute), int(obj.second), 1000 * int(obj.millis)),
    'Json': lambda obj: ujson.loads(obj.json)
})

model = mm.model_from_file(sys.argv[1])


def dict_get(dct, *path):
    for p in path:
        if p in dct:
            dct = dct[p]
        else:
            return None

    return dct


@singledispatch
def process(line, lineno) -> None:
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
def process_jstest_line(line: mm['JSTestLine'], lineno) -> None:
    if not isinstance(line.log_msg, dict):
        # Not a structured server log.
        return

    log_msg = line.log_msg
    if log_msg.get('c') != 'VECCLOCK':
        # Not the nodeVectorClock log component.
        return

    hostID = getattr(line, 'hostID', None)
    server_msg = log_msg['attr']['msg']
    if 'nodeVectorClockForTest' not in server_msg:
        # TODO: How is this possible?
        logging.warning(f"nodeVectorClockForTest absent from {server_msg}")
        return

    node_vector_clock = server_msg['nodeVectorClockForTest']
    msg_id = log_msg.get('id')

    if msg_id == 202007190:
        # Sending the vector clock with a request or reply.
        shiviz_event = f'Send {server_msg}'
    elif msg_id == 202007191:
        # Receiving the vector clock with a request or reply.
        shiviz_event = f'Receive {server_msg}'
    else:
        return

    print(
        f'event={shiviz_event}, host={hostID.port}, clock={node_vector_clock}')


@process.register
def process_other_line(line: mm['OtherLine'], lineno) -> None:
    pass


for i, line in enumerate(model.lines, start=1):
    try:
        process(line, i)
    except Exception as exc:
        print(f'{exc!r} parsing line {i}: {line}')
