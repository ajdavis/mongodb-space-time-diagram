MongoDB Build Failure Log Visualization With ShiViz
===================================================

Instructions
------------

Clone MongoDB from Jesse's fork and branch:
```
git clone -b node-vector-clock git+git@github.com:ajdavis/mongo.git
```

Build the server and select some JS test to run:

```
python3 buildscripts/resmoke.py run \
    --suites=replica_sets \
    jstests/replsets/prepare_survives_primary_reconfig_failover.js \
    > log.txt
```

Use the script included in this project to generate a ShiViz-compatible log:

```
python3 -m bf_log_to_shiviz log.txt > shiviz.txt
```

Load the ShiViz-compatible log in ShiViz.

