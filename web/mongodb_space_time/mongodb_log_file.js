class MongoDBLogEvent {
  constructor({lineno, hostType, port, struct} = {}) {
    this.lineno = lineno;
    this.hostType = hostType;
    this.port = port;
    this.struct = JSON.parse(struct);
    this.logMessageId = this.get('id');
    const timestampStr = this.get(['t', '$date']);
    this.timestamp = timestampStr ? Date.parse(timestampStr) : null;
  }

  get(path, default_ = null) {
    path = path.split ? path.split('.') : path;
    let obj = this.struct;
    for (const key of path) {
      obj = obj ? obj[key] : null;
    }
    return obj ? obj : default_;
  }
}

class Server {
  constructor({pid, port} = {}) {
    this.pid = pid;
    this.port = port;
    // Map connection ids like "conn3" to remote servers' listening ports.
    this.connections = new Map();
  }

  onConnect(connectionId, remoteListeningPort) {
    this.connections.set(connectionId, remoteListeningPort);
  }

  getPortForConnectionId(connectionId) {
    return this.connections.get(connectionId);
  }
}

class NetworkMessage {
  constructor({sourcePort, targetPort, sendTimestamp, receiveTimestamp, body, isRequest} = {}) {
    this.sourcePort = sourcePort;
    this.targetPort = targetPort;
    this.sendTimestamp = sendTimestamp;
    this.receiveTimestamp = receiveTimestamp;
    this.body = body;
    this.isRequest = isRequest;
  }
}

export default class MongoDBLogFile {
  constructor(text) {
    let events = [];
    const pat = /\[js_test:(?<testName>\w+)] \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[+-]\d{4} (?<hostType>[dsm])(?<port>\d+)\| (?<struct>{.*})/;
    for (const [lineno, line] of text.split("\n").entries()) {
      const match = pat.exec(line);
      if (match === null) {
        continue;
      }

      events.push(new MongoDBLogEvent({
        lineno: lineno,
        hostType: match.groups.hostType,
        port: parseInt(match.groups.port),
        struct: match.groups.struct
      }));
    }

    let wireRequestId2PendingMessage = new Map();
    this.networkMessages = [];
    let pid2server = new Map();
    let port2server = new Map();
    for (const event of events) {
      try {
        if (event.logMessageId === 4615611) {
          // "initAndListen".
          const server = Server({pid: event.struct.attr.pid, port: event.port});
          pid2server.set(server.pid, server);
          port2server.set(server.port, server);
        } else if (event.logMessageId == 51800) {
          // "Client metadata". Relies on SERVER-47922.
          const connectionId = event.struct.ctx; // Like "conn123".
          const remotePid = parseInt(event.struct.attr.doc.application.pid);
          // The shell's pid and others aren't in pid2server, only servers are.
          if (remotePid in pid2server) {
            const remoteServer = pid2server.get(remotePid);
            this.port2server.get(event.port).onConnect(connectionId, remoteServer.port);
          }
        } else if (event.logMessageId == 202007260) {
          // "AsyncDBClient::runCommand sending request", custom message Jesse
          // added in his "space-time-diagram" branch.
          const remotePort = parseInt(event.struct.attr.remote.split(":")[1]);
          // Internal id, not the wire protocol requestId.
          const internalRequestId = event.struct.attr.requestId;
          const wireRequestId = event.struct.attr.wireRequestId;
          wireRequestId2PendingMessage.set(wireRequestId, NetworkMessage({
            sourcePort: event.port,
            remotePort: remotePort,
            sendTimestamp: event.timestamp,
            receiveTimestamp: null,
            body: event.struct.attr.commandArgs,
            isRequest: true
          }));
        } else if (event.logMessageId == 21965) {
          // "About to run the command".
          const wireRequestId = event.struct.attr.requestId;
          const pendingMessage = wireRequestId2PendingMessage.get(wireRequestId);

          pendingMessage.receiveTimestamp = event.timestamp;
        }
      } catch (exc) {
        // TODO: display error to user.
        throw exc;
      }
    }
  }
}
