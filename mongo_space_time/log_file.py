import datetime
import logging
from dataclasses import dataclass
from typing import Dict, List

from textx import metamodel_from_str
import ujson

grammar = r"""
Model: lines+=Line;

Line: JSTestLine | OtherLine;

JSTestLine:
    "[js_test:" js_test_name=ID "] " timestamp=Timestamp
    hostID=HostID log_msg=JsonOrText
;

Timestamp:
    year=/\d{4}/ "-" month=/\d{2}/ "-" day=/\d{2}/ "T"
    hour=/\d{2}/ ":" minute=/\d{2}/ ":" second=/\d{2}/ "." millis=/\d{3}/ 
    /[+-]\d{4}/
;

HostID: hostType=/d|s|m/ port=Port "|"; 

Port: port=/\d+/;

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
    "Json": lambda obj: ujson.loads(obj.json),
    "Port": lambda obj: int(obj.port),
})


@dataclass
class Server:
    pid: int
    port: int

    def __post_init__(self):
        # Map connection ids like "conn3" to remote servers' listening ports.
        self.connections = {}

    def on_connect(self, connection_id, remote_listening_port):
        self.connections[connection_id] = remote_listening_port


@dataclass
class LogLine:
    lineno: int
    line: mm["JSTestLine"]


@dataclass
class LogFile:
    lines: List[LogLine]
    pid_to_server: Dict[int, Server]
    port_to_server: Dict[int, Server]

    @property
    def server_ports(self):
        return self.port_to_server.keys()


def parse_log_file(file_name) -> LogFile:
    lines = []
    pid_to_server = {}
    port_to_server = {}

    for lineno, line in enumerate(mm.model_from_file(file_name).lines):
        try:
            if (not isinstance(line, mm["JSTestLine"])
                or not isinstance(line.log_msg, dict)):
                # Not a structured server log.
                continue

            log_msg = line.log_msg
            msg_id = log_msg["id"]
            if msg_id == 4615611:
                # "initAndListen".
                server = Server(
                    log_msg['attr']['pid'],
                    int(log_msg['attr']['port']))
                pid_to_server[server.pid] = server
                port_to_server[server.port] = server
            elif msg_id == 51800:
                # "Client metadata". Relies on SERVER-47922.
                connection_id = log_msg['ctx']  # Like "conn123".
                remote_pid = int(
                    log_msg['attr']['doc']['application']['pid'])

                if remote_pid in pid_to_server:
                    remote_server = pid_to_server[remote_pid]
                    server = port_to_server[line.hostID.port]
                    server.on_connect(
                        connection_id, remote_server.port)

            lines.append(LogLine(lineno, line))
        except Exception:
            logging.exception(f"Processing line {lineno}: {line.log_msg}")

    return LogFile(lines, pid_to_server, port_to_server)
