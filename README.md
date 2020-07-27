MongoDB Resmoke Test Space-Time Diagram
=======================================

Visualizes the execution of a MongoDB resmoke.py test (e.g., a test run in
Evergreen) as a "space-time diagram" in the style of Lamport's "Time, clocks,
and the ordering of events in a distributed system".

Instructions
------------

Clone MongoDB from Jesse's fork and branch:
```
git clone -b node-vector-clock git+git@github.com:ajdavis/mongo.git
```

Build the server and select some JS test to run with increased logging:

```
python3 buildscripts/resmoke.py run \
    --suites=replica_sets \
    --mongodSetParameters "{'logComponentVerbosity': '{\"command\": 2, \"network\": 2}'}" \
    jstests/replsets/prepare_survives_primary_reconfig_failover.js \
    > log.txt
```

Display the included `web/index.html` with some web server, visit the page,
click "Choose File", and select the log file you just created.
