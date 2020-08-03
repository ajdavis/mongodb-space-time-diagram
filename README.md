MongoDB Resmoke Test Space-Time Diagram
=======================================

Visualizes the execution of a MongoDB resmoke.py test (e.g., a test run in
Evergreen) as a "space-time diagram" in the style of Lamport's "Time, clocks,
and the ordering of events in a distributed system".

Instructions
------------

Enable tcpdump on all ports your cluster will use on the local host, e.g.:

```
sudo tcpdump -Xs0 -Nnpi lo -w ~/mongo.pcap "port (27017 or 27018 or 27019)"
```

Select some JS test to run:

```
python3 buildscripts/resmoke.py run --suites=replica_sets \
    jstests/replsets/prepare_survives_primary_reconfig_failover.js
```

Kill `tcpdump` and run:

```
python3 process-logs.py ~/mongo.pcap out.space-time
```

Display the included `web/index.html` with some web server, visit the page,
click "Choose File", and select the `out.space-time` file you just created.
