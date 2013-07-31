High contention counter for Foundation DB
=================

Java port of Foundation's python counter script.

You will need to update the pom to get the fdb jar from somewhere - I pull it from a local nexus repository.

The unit test is a pretty naive microbenchmark, and the time isn't exact due to the wait period in awaitility.