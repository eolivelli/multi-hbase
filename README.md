# A client for HBase 0.94 and HBase 1.2.4

This project is a simple proof-of-concept on how to build a client which is able to connect to an HBase 0.94 cluster and an HBase 1.2.4 cluster.

HBase 0.94 and 1.2 clients are not compatible and cannot work inside the same JMV, so I have shaded most of the 0.94 client and a little of the 1.2 client in order to build a Java library
which is able to read data from 0.94 and write to 1.2.

My principal use case is to migrate data from 0.94 to 1.2 without any downtime.

A "real" application of this 'trick' will be able to read/write from/to the legacy cluster and switch step by step to read/write from/to the new version cluster.

I think the idea of migrating live data from 0.94 to 1.2 is valuable too because you do not need to created backups and apply conversions to HDFS data.

This project contains a [very simple utility](multi-hbase-client/src/test/java/MigrateHBaseDataExample.java) which performs a Scan from a table of a 0.94 cluster and executes Puts on the destination cluster, this will be a 'logical' conversion
of all the data but in my case is will be enough.

The 1.2.4 client was not tweaked much, and so this is gives hope to be able to use this kind of approach to future releases.

## Main tricks

In order to have a working "shaded" 0.94 client I had to:

* strip out the hbase_default.xml file from both the 0.94 client and the 1.2.4 client

* the instantiation of HBaseConfiguration is to be forced by using the hbase.defaults.for.version property

* some classes cannot be shaded due to the protocol, as the full org.apache.hadoop.hbase.ipc package

* HBase/Hadoops use JAAS in order to access to the current user of the SO, this was the most headache source, because of "shaded" references to ShellBasedUnixGroupsMapping, UnixLoginModule and so on

* another source of troubles is the use of SLF4J, for which I had to keep both a shaded version and the real version


Any comment or suggestion will be very appreciated. Please use the bug tracker of this project or submit PRs