cassandra-sharp is a .NET client for Apache Cassandra.
It is able to interact with the Cassandra 0.8.x version using thrift API.
cassandra-sharp has no unit test for the moment. Sorry about that. They will be committed soon.

This project is open sourced under the Apache License 2.0.

In its current implementation, cassandra-sharp acts as a connection provider giving access to the underlying 
Cassandra.Client interface. It's pretty raw. Next versions will include:
- unit tests
- IDictionary wrapping
- recovery of banned endpoints

To compile, use a command line and run the following commands:
- Generate.bat (generate .NET thrift interface in Apache.Cassandra)
- Build.bat (build both Debug/Release, binaries will be found under bin/$(Configuration)
- Merge.bat (CassandraSharpCore, Apache.Cassandra & Thrift assemblies are merged together into CassandraSharp
  using ilmerge)
