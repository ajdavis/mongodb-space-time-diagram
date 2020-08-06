import binascii
import datetime
import heapq
import io
import struct
from dataclasses import dataclass
from operator import attrgetter

import bson
import pyshark
import snappy
import typing


@dataclass
class RawMessage:
    src: str
    dst: str
    data: bytes
    start_timestamp: datetime.datetime
    end_timestamp: datetime.datetime


@dataclass
class TCPStream:
    client: str
    server: str
    messages: typing.List[RawMessage]


def decode_payload(payload):
    # TODO: can I get the raw data directly?
    return binascii.unhexlify(payload.replace(':', ''))


def get_streams(file_name):
    """Get TCPStreams from a pcap file."""
    cap = pyshark.FileCapture(file_name)
    streams = {}
    for packet in cap:
        if not hasattr(packet.tcp, 'payload'):
            continue

        src = f'{packet.ip.addr}:{packet.tcp.srcport}'
        dst = f'{packet.ip.dst}:{packet.tcp.dstport}'
        payload = decode_payload(packet.tcp.payload)

        if packet.tcp.stream not in streams:
            # Stream's first packet determines which peer is the client/server.
            message = RawMessage(
                src,
                dst,
                payload,
                packet.sniff_time,
                packet.sniff_time)
            streams[packet.tcp.stream] = TCPStream(src, dst, [message])
        else:
            stream = streams[packet.tcp.stream]
            last_message = stream.messages[-1]
            if last_message.src == src and last_message.dst == dst:
                last_message.data += payload
                last_message.end_timestamp = packet.sniff_time
            else:
                stream.messages.append(RawMessage(
                    src,
                    dst,
                    payload,
                    packet.sniff_time,
                    packet.sniff_time))

    # TODO: detect when a stream ends, yield and discard it early.
    yield from streams.values()


@dataclass
class MongoMessage:
    src: str
    dst: str
    request_id: int
    response_to: int
    body: dict
    start_timestamp: datetime.datetime
    end_timestamp: datetime.datetime

    # Sort by (start_timestamp, clusterTime.time, clusterTime.inc).
    sort_key: typing.Tuple[datetime.datetime, int, int] = 0, 0, 0

    # Filled in by filter_mongo_messages().
    requester_pid: typing.Optional[int] = None
    requester_application_name: typing.Optional[str] = None

    def __post_init__(self):
        cluster_time = self.body.get('$clusterTime')
        if cluster_time:
            op_time = cluster_time['clusterTime']
            self.sort_key = (self.start_timestamp, op_time.time, op_time.inc)
        else:
            self.sort_key = (self.start_timestamp, 0, 0)

    @property
    def is_request(self):
        # The first requestId and responseTo are 0, use 'ok' for backup.
        return self.response_to == 0 and 'ok' not in self.body

    @property
    def command_name(self):
        if not self.is_request:
            return None

        return next(iter(self.body))

    def safe_get(self, path):
        doc = self.body
        for p in path.split('.'):
            if p not in doc:
                return None

            doc = doc[p]

        return doc


def get_mongo_messages(file_name):
    """Generate MongoMessage instances from a pcap file."""
    unpack_byte = struct.Struct("<B").unpack
    unpack_int = struct.Struct("<I").unpack
    unpack_long = struct.Struct("<Q").unpack
    unpack_message_header = struct.Struct("<iiii").unpack

    def read_remainder():
        return f.read(msg_len - (f.tell() - message_start))

    def unpack_string(f):
        s = b""
        while True:
            c = f.read(1)
            if c == b'\x00':
                return s.decode()

            s += c

    def unpack_bson(bio):
        bson_bytes = bio.read(4)
        bson_length = unpack_int(bson_bytes)[0]
        bson_bytes += bio.read(bson_length - 4)
        try:
            return bson.decode(bson_bytes)
        except bson.InvalidBSON as exc:
            # The "electionTime" timestamp is usually out of range.
            return {'error': str(exc)}

    def mongo_message_from_op_msg(data):
        bio = io.BytesIO(data)
        op_msg_flags = unpack_int(bio.read(4))[0]
        checksum_present = op_msg_flags & 0x1
        sections_end = len(data) - (4 if checksum_present else 0)

        body = {}

        while bio.tell() < sections_end:
            payload_type = unpack_byte(bio.read(1))[0]
            if payload_type == 0:
                # Body section.
                body.update(unpack_bson(bio))
            else:
                # Document sequence.
                assert payload_type == 1
                section_start = bio.tell()
                section_size = unpack_int(bio.read(4))[0]
                sequence_identifier = unpack_string(bio)
                documents = []
                while bio.tell() < section_start + section_size:
                    documents.append(unpack_bson(bio))

                body[sequence_identifier] = documents

        if op_msg_flags & 0x1:
            checksum = bio.read(4)

        return MongoMessage(
            src=src_port,
            dst=dst_port,
            request_id=request_id,
            response_to=response_to,
            body=body,
            start_timestamp=message.start_timestamp,
            end_timestamp=message.end_timestamp)

    for stream in get_streams(file_name):
        for message in stream.messages:
            src_port = int(message.src.split(':')[-1])
            dst_port = int(message.dst.split(':')[-1])

            f = io.BytesIO(message.data)
            # MongoDB wire protocol.
            message_start = f.tell()
            msg_len, request_id, response_to, op_code = unpack_message_header(
                f.read(16))

            if op_code == 2004:
                # OP_QUERY.
                flags = unpack_int(f.read(4))[0]
                ns = unpack_string(f)
                number_to_skip = unpack_int(f.read(4))[0]
                number_to_return = unpack_int(f.read(4))[0]
                query = bson.decode(read_remainder())
                yield MongoMessage(
                    src=src_port,
                    dst=dst_port,
                    request_id=request_id,
                    response_to=response_to,
                    body=query,
                    start_timestamp=message.start_timestamp,
                    end_timestamp=message.end_timestamp)
            elif op_code == 1:
                # OP_REPLY.
                flags = unpack_int(f.read(4))[0]
                cursor_id = unpack_long(f.read(8))[0]
                starting_from = unpack_int(f.read(4))[0]
                number_returned = unpack_int(f.read(4))[0]
                reply = bson.decode(read_remainder())
                yield MongoMessage(
                    src=src_port,
                    dst=dst_port,
                    request_id=request_id,
                    response_to=response_to,
                    body=reply,
                    start_timestamp=message.start_timestamp,
                    end_timestamp=message.end_timestamp)
            elif op_code == 2012:
                # OP_COMPRESSED.
                compressed_op_code = unpack_int(f.read(4))[0]
                compressed_length = unpack_int(f.read(4))[0]
                compressor_id = unpack_byte(f.read(1))[0]
                data = read_remainder()
                # TODO: use compressor_id to determine whether it's Snappy.
                op_msg = snappy.uncompress(data)
                yield mongo_message_from_op_msg(op_msg)
            elif op_code == 2013:
                # OP_MSG.
                yield mongo_message_from_op_msg(f.read(msg_len - 16))


def merge_pcaps(*file_names):
    """Get MongoMessages sorted by start time, clusterTime from pcap files."""
    generators = (get_mongo_messages(file_name)
                  for file_name in file_names)

    return heapq.merge(*generators, key=attrgetter('sort_key'))


def parse_pcap_files(*file_names):
    """Generate intra-cluster MongoMessage instances from pcap files."""
    @dataclass
    class Client:
        src: str
        pid: int
        application_name: str

    clients = {}
    # Intra-cluster requests for which we haven't seen a reply.
    requests = {}

    # Traffic recordings include requests that the server *receives* and the
    # replies it sends. It omits the server's requests, and replies it receives.
    for mongo_message in merge_pcaps(*file_names):
        if mongo_message.is_request:
            application_name = mongo_message.safe_get('client.application.name')
            pid = mongo_message.safe_get('client.application.pid')
            if application_name and (
                application_name.endswith('mongod')
                or application_name.endswith('mongos')
            ):
                # "isMaster" handshake. We'll overwrite if a port's reused.
                clients[mongo_message.src] = Client(
                    mongo_message.src, int(pid), application_name)

            client = clients.get(mongo_message.src)
            if not client:
                # Request from a non-server.
                continue

            mongo_message.requester_pid = client.pid
            mongo_message.requester_application_name = client.application_name
            requests[mongo_message.request_id] = mongo_message
            yield mongo_message
        else:
            request = requests.pop(mongo_message.response_to, None)
            if request is None:
                # Reply to a non-server.
                continue

            mongo_message.requester_pid = request.requester_pid
            mongo_message.requester_application_name = \
                request.requester_application_name

            yield mongo_message

    if requests:
        print(f'{len(requests)} requests without replies')
