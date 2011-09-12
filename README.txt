cassandra-sharp is a .NET client for Apache Cassandra released under the Apache License 2.0.
It is able to interact with the Cassandra 0.8.x version using thrift API.

In its current implementation, cassandra-sharp acts mostly as a connection provider giving access to the underlying 
Cassandra.Client interface. It's pretty raw. Next versions will include:
* IDictionary wrapping
* recovery of banned endpoints

To compile, use a command line and run the following commands:
GenerateVersion.bat <VersionNumber>

You will find a package named "cassandra-sharp-bin-<VersionNumber>.zip" in the root folder.