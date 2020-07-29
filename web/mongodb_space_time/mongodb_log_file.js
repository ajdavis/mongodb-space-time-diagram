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

function bigUint64(view, offset) {
  const pair = view.getUint64(offset);
  return BigInt(2 ** 32) * BigInt(pair.hi) + BigInt(pair.lo);
}

function cString(view, offset) {
  let rv = "";
  let i = offset;
  while (view.getChar(i) !== '\0') {
    rv += view.getChar(i);
    ++i;
  }

  return rv;
}

const trafficRecordingStructure = {
  'jBinary.littleEndian': true,

  DynamicArray: jBinary.Template({
    setParams: function (itemType) {
      this.baseType = {
        // using built-in type
        length: 'uint16',
        // using complex built-in type with simple argument and argument from another field
        values: ['array', itemType, 'length']
      };
    },
    read: function () {
      return this.baseRead().values;
    },
    write: function (values) {
      // TODO: remove this method
      this.baseWrite({
        length: values.length,
        values: values
      });
    }
  }),

  MessageItem: {
    length: 'uint32',
    packetId: 'uint64',
    source: ['string0'],
    target: ['string0'],
    timestamp: 'uint64',
    packetOrder: 'uint64',
    packet: 'DynamicArray'
  },

  // aliasing FileItem[] as type of entire File
  File: ['array', 'FileItem']
};

export default class MongoDBTrafficRecording {
  constructor(data) {
    this.networkMessages = [];
    let pid2server = new Map();
    let port2server = new Map();
    let view = new jDataView(data, 0, data.length, true /* littleEndian */);

    let frameStart = 0;
    while (true) {
      let offset = frameStart;
      const frameSize = view.getUint32(offset);
      const packetId = bigUint64(view, offset += 4);
      const source = cString(view, offset += 8);
      const target = cString(view, offset += source.length + 1);
      const timestamp = bigUint64(view, offset += target.length + 1);
      const packetOrder = bigUint64(view, offset += 8);
      try {
        const packet = view.getBytes(frameSize - offset + frameStart, offset);
      } catch (e) {
        console.log(e);
      }
      frameStart += frameSize;
    }

    console.log(`${this.networkMessages.length} messages`);
  }
}
