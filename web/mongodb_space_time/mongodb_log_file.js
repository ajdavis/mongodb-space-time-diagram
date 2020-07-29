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
    // Map wire protocol messageId to outgoing messages.
    this.outgoingRequests = new Map();
    this.outgoingReplies = new Map();
  }

  onConnect(connectionId, remoteListeningPort) {
    this.connections.set(connectionId, remoteListeningPort);
  }

  onOutgoingRequest(networkMessage) {
    this.outgoingRequests.set(networkMessage.messageId, networkMessage);
  }

  popOutgoingRequest(messageId) {
    const networkMessage = this.outgoingRequests.get(messageId);
    this.outgoingRequests.delete(messageId);
    return networkMessage;
  }

  onOutgoingReply(networkMessage) {
    this.outgoingReplies.set(networkMessage.messageId, networkMessage);
  }

  popOutgoingReply(messageId) {
    const networkMessage = this.outgoingReplies.get(messageId);
    this.outgoingReplies.delete(messageId);
    return networkMessage;
  }

  getPortForConnectionId(connectionId) {
    return this.connections.get(connectionId);
  }
}

class NetworkMessage {
  constructor({sourcePort, targetPort, sendTimestamp, receiveTimestamp, body, isRequest, messageId, responseTo} = {}) {
    this.sourcePort = sourcePort;
    this.targetPort = targetPort;
    this.sendTimestamp = sendTimestamp;
    this.receiveTimestamp = receiveTimestamp;
    this.body = body;
    this.isRequest = isRequest;
    this.messageId = messageId;
    this.responseTo = responseTo;
  }
}

function portFromRemote(remote) {
  return parseInt(remote.split(":")[1]);
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

    this.networkMessages = [];
    let pid2server = new Map();
    let port2server = new Map();
    for (const event of events) {
      if (event.logMessageId === 4615611) {
        // "initAndListen".
        const server = new Server({
          pid: event.struct.attr.pid,
          port: event.port
        });
        pid2server.set(server.pid, server);
        port2server.set(server.port, server);
      } else if (event.logMessageId == 51800) {
        // "Client metadata". Relies on SERVER-47922.
        const connectionId = event.struct.ctx; // Like "conn123".
        if (!event.struct.attr.doc.hasOwnProperty("application")) {
          continue;
        }
        if (!event.struct.attr.doc.application.hasOwnProperty("pid")) {
          continue;
        }
        const remotePid = parseInt(event.struct.attr.doc.application.pid);
        // The shell's pid and others aren't in pid2server, only servers are.
        if (pid2server.has(remotePid)) {
          const remoteServer = pid2server.get(remotePid);
          port2server.get(event.port).onConnect(connectionId, remoteServer.port);
        }
      } else if (event.logMessageId == 202007260) {
        // "AsyncDBClient::runCommand sending request", custom message Jesse
        // added in his "space-time-diagram" branch.
        const recipientPort = portFromRemote(event.struct.attr.remote);
        // This is the wire protocol requestId.
        const messageId = event.struct.attr.messageId;
        const sendingServer = port2server.get(event.port);
        const networkMessage = new NetworkMessage({
          sourcePort: event.port,
          remotePort: recipientPort,
          sendTimestamp: event.timestamp,
          receiveTimestamp: null,
          body: event.struct.attr.commandArgs,
          isRequest: true,
          messageId: messageId,
          responseTo: null
        });
        sendingServer.onOutgoingRequest(networkMessage);
        this.networkMessages.push(networkMessage);
      } else if (event.logMessageId == 21965) {
        // "About to run the command", we've received a request.
        const recipientServer = port2server.get(event.port);
        const connectionId = event.struct.attr.ctx;
        const sourcePort = recipientServer.getPortForConnectionId(connectionId);
        if (sourcePort === undefined) {
          // The request is coming from a client, not a peer server.
          continue;
        }

        const messageId = event.struct.attr.messageId;
        const sendingServer = port2server.get(sourcePort);
        const pendingMessage = sendingServer.popOutgoingRequest(messageId);
        pendingMessage.receiveTimestamp = event.timestamp;
      } else if (event.logMessageId == 202007262) {
        // "_processMessage replying", custom message Jesse
        // added in his "space-time-diagram" branch.
        const replyingServer = port2server.get(event.port);
        const connectionId = event.struct.ctx;
        if (replyingServer.getPortForConnectionId(connectionId) === undefined) {
          // The reply is going to a client, not a peer server.
          continue;
        }
        const messageId = event.struct.attr.messageId;
        const networkMessage = new NetworkMessage({
          sourcePort: event.port,
          remotePort: portFromRemote(event.struct.attr.remote),
          sendTimestamp: event.timestamp,
          receiveTimestamp: null,
          body: null,
          isRequest: false,
          messageId: messageId,
          responseTo: event.struct.attr.responseTo
        });
        replyingServer.onOutgoingReply(networkMessage);
        this.networkMessages.push(networkMessage);
      } else if (event.logMessageId == 202007261) {
        // "AsyncDBClient::runCommandRequest got reply", custom message Jesse
        // added in his "space-time-diagram" branch.
        const remotePort = portFromRemote(event.struct.attr.remote);
        const messageId = event.struct.attr.messageId;
        const sendingServer = port2server.get(remotePort);
        const pendingMessage = sendingServer.popOutgoingReply(messageId);
        pendingMessage.receiveTimestamp = event.timestamp;
        pendingMessage.body = event.struct.attr.response;
      }
    }

    console.log(`${this.networkMessages.length} messages`);
  }
}
