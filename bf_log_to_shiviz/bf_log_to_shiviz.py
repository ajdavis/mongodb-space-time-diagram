import datetime
import logging
import sys
from typing import Generator

from textx import metamodel_from_str
import ujson

logging.basicConfig()

grammar = r"""
Model: lines+=Line;

Line: JSTestLine | OtherLine;

JSTestLine:
    "[js_test:" js_test_name=ID "] " timestamp=Timestamp
    hostID=/(d|s|m)\d+\|/ log_msg=JsonOrText
;

Timestamp:
    year=/\d{4}/ "-" month=/\d{2}/ "-" day=/\d{2}/ "T"
    hour=/\d{2}/ ":" minute=/\d{2}/ ":" second=/\d{2}/ "." millis=/\d{3}/ 
    /[+-]\d{4}/
;

JsonOrText: Json | Text;

Json: json=/\{.*\}$/;

Text: text=/.*$/;

OtherLine: message=/.*$/;
"""

mm = metamodel_from_str(grammar)
mm.register_obj_processors({
    "Timestamp": lambda obj: datetime.datetime(
        int(obj.year), int(obj.month), int(obj.day), int(obj.hour),
        int(obj.minute), int(obj.second), 1000 * int(obj.millis)),
    "Json": lambda obj: ujson.loads(obj.json)
})


# https://github.com/DistributedClocks/shiviz/wiki
# ShiViz parses the log using a user-specified regular expression. The regular
# expression must contain three capture groups:
#
# event: The event message
# host: The host / process for the event
# clock: The vector clock, in JSON {"host": timestamp} format. The local host
# must be represented in the vector clock (i.e. the host specified in the host
# capture groups must be one of the hosts in the vector clock).
class ShiVizEvent:
    __slots__ = ("description", "host", "clock")

    def __init__(self, description, host, clock):
        self.description = description
        self.host = host
        self.clock = clock


def shiviz_events(model) -> Generator[ShiVizEvent, None, None]:
    for lineno, line in enumerate(model.lines):
        try:
            if (not isinstance(line, mm["JSTestLine"])
                    or not isinstance(line.log_msg, dict)):
                # Not a structured server log.
                continue

            log_msg = line.log_msg
            if log_msg.get("c") != "VECCLOCK":
                # Not the nodeVectorClock log component.
                continue

            msg_id = log_msg["id"]
            host = log_msg["attr"]["self"]
            node_vector_clock = log_msg["attr"]["nodeVectorClock"]

            if msg_id == 202007190:
                # Sending the vector clock with a request or reply.
                description = f"Send {log_msg['attr']['message']}"
            elif msg_id == 202007191:
                # Receiving the vector clock with a request or reply.
                description = f"Receive a node vector clock"
            else:
                logging.warning(
                    f'Unexpected log message id in component VECCLOCK: {msg_id}'
                )
                continue

            yield ShiVizEvent(description, host, node_vector_clock)
        except Exception:
            logging.exception(f"Processing line {lineno}: {line.log_msg}")


def read_bf_log(filename):
    model = mm.model_from_file(filename)

    # ShiViz wants all clocks to include all hosts. First get the set of hosts.
    hosts = set()
    for event in shiviz_events(model):
        hosts.add(event.host)

    print(r'event=(?<event>.*?), host=(?<host>.*?), clock=(?<clock>.*)')

    # Default missing hosts" clocks to -1.
    default_clock = {host_id: 1 for host_id in hosts}
    for event in shiviz_events(model):
        clock = default_clock.copy()
        clock.update(event.clock)
        print(f"event={event.description},"
              f" host={event.host},"
              f" clock={ujson.dumps(clock)}")


if __name__ == "__main__":
    # TODO: argparse.
    read_bf_log(sys.argv[1])
