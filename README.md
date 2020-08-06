MongoDB Resmoke Test Space-Time Diagram
=======================================

Visualizes the execution of a MongoDB resmoke.py test (e.g., a test run in
Evergreen) as a "space-time diagram" in the style of Lamport's "Time, clocks,
and the ordering of events in a distributed system".

Instructions
------------

Build MongoDB from my fork and branch:

https://github.com/ajdavis/mongo/tree/space-time-diagram-client-metadata

Enable tcpdump on all ports your cluster will use on the local host, e.g.:

```
sudo tcpdump -Xs0 -Nnpi any -w ~/mongo.pcap "port (27017 or 27018 or 27019)"
```

Select some JS test to run:

```
python3 buildscripts/resmoke.py run --suites=replica_sets \
    jstests/replsets/prepare_survives_primary_reconfig_failover.js \
    > prepare_survives_primary_reconfig_failover.log
```

Terminate `tcpdump` with Control-C, then run:

```
python3 process-logs.py mongo.pcap prepare_survives_primary_reconfig_failover.log > shiviz.txt
```

Open [ShiViz](https://bestchai.bitbucket.io/shiviz/) and upload the `shiviz.txt`
you just created.
