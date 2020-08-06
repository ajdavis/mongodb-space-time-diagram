import argparse
import logging

from bson import json_util

from mongo_space_time.log_file import parse_log_file
from mongo_space_time.pcap import parse_pcap_files
from mongo_space_time.shiviz import ShiVizEvent, print_shiviz_input_file


def main():
    logging.basicConfig()

    parser = argparse.ArgumentParser()
    parser.add_argument('file_name', nargs='+')
    args = parser.parse_args()

    pcaps = []
    log_files = []

    for file_name in args.file_name:
        if file_name.endswith('.pcap'):
            pcaps.append(file_name)
        elif file_name.endswith('.log'):
            log_files.append(file_name)
        else:
            parser.error('filenames must end in .pcap or .log')

    if len(log_files) != 1:
        parser.error('pass exactly one log file')

    parsed_log_file = parse_log_file(log_files[0])
    mongo_messages = list(parse_pcap_files(*pcaps))
    shiviz_events = []
    # ShiViz needs the clock hands to be strings.
    vector_clock = {str(port): 0 for port in parsed_log_file.server_ports}

    for message in mongo_messages:
        if message.is_request:
            src_server = parsed_log_file.pid_to_server[message.requester_pid]
            dst_server = parsed_log_file.port_to_server[message.dst]
        else:
            # The replying server is the source, the client is the destination.
            src_server = parsed_log_file.port_to_server[message.src]
            dst_server = parsed_log_file.pid_to_server[message.requester_pid]

        vector_clock[str(src_server.port)] += 1
        direction = "request" if message.is_request else "reply"
        response_to = ("" if message.is_request
                       else f" response_to:{message.response_to}")

        shiviz_events.append(ShiVizEvent(
            description=f'{direction}'
                        f' id:{message.request_id}{response_to}'
                        f' {json_util.dumps(message.body)}',
            host=str(src_server.port),
            clock=vector_clock.copy()))

        vector_clock[str(dst_server.port)] += 1
        shiviz_events.append(ShiVizEvent(
            description=f'receive {direction} {message.request_id}',
            host=str(dst_server.port),
            clock=vector_clock.copy()))

    print_shiviz_input_file(shiviz_events)


if __name__ == '__main__':
    main()
