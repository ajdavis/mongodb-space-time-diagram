import {Interactive} from "https://vectorjs.org/index.js";
import {MongoDBTrafficRecording, trafficRecordingStructure} from "./mongodb_space_time/mongodb_log_file.js";

let file_input = document.getElementById("file-input");

file_input.addEventListener('click', (e) => {
  // Clear the file input value whenever 'Choose File' is clicked.
  e.target.value = "";
});

file_input.addEventListener('change', (e) => {
  jBinary.load(e.target.files[0], trafficRecordingStructure ).then(function (data) {
    new MongoDBTrafficRecording(data);
  });
});

class Event {
  constructor(startTS, endTS, source, target, message) {
    this.startTS = startTS;
    this.endTS = endTS;
    this.source = source;
    this.target = target;
    this.message = message;
  }
}

let serverIds = ["20020", "20021"];
let events = [
  new Event(0, 1, "20020", "20021", "foo"),
  new Event(3, 4, "20021", "20020", "bar"),
  new Event(4, 4, "20021", null, "baz")
];

let interactive = new Interactive("interactive");
interactive.border = true;

let timeLines = [];

function serverX(serverId) {
  return serverIds.indexOf(serverId) * 100 + 50;
}

function timestampY(timestamp) {
  return timestamp * 50 + 50;
}

for (const serverId of serverIds) {
  const x = serverX(serverId);
  let label = interactive.text(x, 25, serverId);
  // Center the server label.
  label.x -= label.getBoundingBox().width / 2;
  let line = interactive.line(x, timestampY(0), x, timestampY(10));
  timeLines.push(line);
}

for (const event of events) {
  if (event.target !== null) {
    console.assert(event.endTS !== null);

    let arrow = interactive.line(
      serverX(event.source),
      timestampY(event.startTS),
      serverX(event.target),
      timestampY(event.endTS),
    );
  } else {
    let dot = interactive.circle(
      serverX(event.source),
      timestampY(event.startTS),
      5
    );
  }
}
