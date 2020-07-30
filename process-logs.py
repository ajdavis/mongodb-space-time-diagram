import heapq
import re
import struct
from dataclasses import dataclass

import bson


@dataclass
class TrafficRecord:
    """One traffic record in a file produced by mongod or mongos.

    See traffic_recorder.cpp.
    """
    local: str
    remote: str
    timestamp: int
    order: int
    request_id: int
    response_to: int
    message: dict

    @property
    def is_request(self):
        # The first requestId and responseTo are 0, use 'ok' for backup.
        return self.response_to == 0 and 'ok' not in self.message

    @property
    def command_name(self):
        if not self.is_request:
            return None

        return next(iter(self.message))

    @property
    def application_name(self):
        if not self.is_request:
            return None

        return self._safe_get('client.application.name')

    def _safe_get(self, path):
        doc = self.message
        for p in path.split('.'):
            if p not in doc:
                return None

            doc = doc[p]

        return doc


@dataclass
class Transmission:
    """The emission and receipt of a network message."""
    source: str
    target: str
    send_timestamp: int
    recv_timestamp: int
    is_reply: bool
    body: dict


def get_traffic_records(file_name):
    """Generate TrafficRecord instances from a traffic recording stream.

    Traffic recording format:

    uint32_t    recordLen; // the length of this record: message+metadata
    uint64_t    id;        // the transport::Session id
    cstr        local;     // the local address
    cstr        remote;    // the remote address
    uint64_t    date;      // in millis since epoch
    uint64_t    order;     // always 1?
    Message     message;   // The on wire encoding
    """
    local_port_matches = re.findall(r'\d+', file_name)
    if len(local_port_matches) != 1:
        raise ValueError(
            f"Couldn't determine server port number from file name"
            f" '{file_name}, it should include exactly one number")

    local_port = local_port_matches[0]
    f = open(file_name, 'rb')

    unpack_byte = struct.Struct("<B").unpack
    unpack_int = struct.Struct("<I").unpack
    unpack_long = struct.Struct("<Q").unpack
    unpack_message_header = struct.Struct("<iiii").unpack

    def unpack_string():
        s = b""
        while True:
            c = f.read(1)
            if c == b'\x00':
                return s.decode()

            s += c

    def unpack_bson():
        bson_bytes = f.read(4)
        bson_length = unpack_int(bson_bytes)[0]
        bson_bytes += f.read(bson_length - 4)
        return bson.decode(bson_bytes)

    while True:
        # Traffic record header written by traffic_recorder.cpp.
        record_len_bytes = f.read(4)
        if len(record_len_bytes) == 0:
            # Finished reading the file.
            return

        record_len = unpack_int(record_len_bytes)[0]
        packet_id = unpack_long(f.read(8))[0]
        local = unpack_string()
        remote = unpack_string()
        timestamp = unpack_long(f.read(8))[0]
        order = unpack_long(f.read(8))[0]

        # MongoDB wire protocol.
        message_start = f.tell()
        msg_len, request_id, response_to, op_code = unpack_message_header(
            f.read(16))

        if op_code == 2004:
            # OP_QUERY.
            flags = unpack_int(f.read(4))[0]
            ns = unpack_string()
            number_to_skip = unpack_int(f.read(4))[0]
            number_to_return = unpack_int(f.read(4))[0]
            query = bson.decode(f.read(msg_len - (f.tell() - message_start)))
            yield TrafficRecord(
                local=local,
                remote=remote,
                timestamp=timestamp,
                order=order,
                request_id=request_id,
                response_to=response_to,
                message=query)
        elif op_code == 1:
            # OP_REPLY.
            flags = unpack_int(f.read(4))[0]
            cursor_id = unpack_long(f.read(8))[0]
            starting_from = unpack_int(f.read(4))[0]
            number_returned = unpack_int(f.read(4))[0]
            reply = bson.decode(f.read(msg_len - (f.tell() - message_start)))
            yield TrafficRecord(
                local=local,
                remote=remote,
                timestamp=timestamp,
                order=order,
                request_id=request_id,
                response_to=response_to,
                message=reply)
        elif op_code == 2012:
            assert False, "TODO: OP_COMPRESSED"
        elif op_code == 2013:
            # OpMsg.
            flags = unpack_int(f.read(4))[0]
            checksum_present = flags & 0x1
            sections_end = (message_start + msg_len
                            - (4 if checksum_present else 0))

            body = {}

            while f.tell() < sections_end:
                payload_type = unpack_byte(f.read(1))[0]
                if payload_type == 0:
                    # Body section.
                    body = unpack_bson()
                else:
                    # Document sequence.
                    assert payload_type == 1
                    assert False, "untested, probably needs work"
                    section_start = f.tell()
                    section_size = unpack_int(f.read(4))[0]
                    sequence_identifier = unpack_string()
                    documents = []
                    while f.tell() < section_start + section_size:
                        documents.append(unpack_bson())

                    body[sequence_identifier] = documents

            if flags & 0x1:
                checksum = f.read(4)

            yield TrafficRecord(
                local=local,
                remote=remote,
                timestamp=timestamp,
                order=order,
                request_id=request_id,
                response_to=response_to,
                message=body)


def merge_traffic_recordings(*file_names):
    generators = (get_traffic_records(file_name)
                  for file_name in file_names)
    return heapq.merge(*generators, key=lambda record: record.timestamp)


def main(*file_names):
    # Source of peer connections.
    server_source_ports = set()
    requests = {}
    for record in merge_traffic_recordings(*file_names):
        if record.application_name:
            if (record.application_name.endswith('mongod')
                    or record.application_name.endswith('mongos')):
                server_source_ports.add(record.remote)

        if record.is_request and record.remote in server_source_ports:
            requests[record.request_id] = record


main('traffic-recorder-20020', 'traffic-recorder-20021')
