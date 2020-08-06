import ujson
from dataclasses import dataclass
from typing import Dict


# https://github.com/DistributedClocks/shiviz/wiki
# ShiViz parses the log using a user-specified regular expression. The regular
# expression must contain three capture groups:
#
# event: The event message
# host: The host / process for the event
# clock: The vector clock, in JSON {"host": timestamp} format. The local host
# must be represented in the vector clock (i.e. the host specified in the host
# capture groups must be one of the hosts in the vector clock).
#
# We use the server request or reply as "event" and the port number for "host".


@dataclass
class ShiVizEvent:
    description: str
    host: str
    clock: Dict[str, int]


def print_shiviz_input_file(shiviz_events):
    # ShiViz uses the first line of its input file as the matching regex.
    print(r'(?<host>\S*) (?<clock>{.*})\n(?<event>.*)')
    # Shiviz uses the second line as the "Multiple executions regular expression
    # delimiter", leave it blank.
    print()

    for event in shiviz_events:
        print(f'{event.host} {ujson.dumps(event.clock)}\n{event.description}')
