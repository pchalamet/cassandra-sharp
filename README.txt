cassandra-sharp is a .NET client for Apache Cassandra released under the Apache License 2.0.
It is able to interact with the Cassandra 0.8.x version using thrift API.

In its current implementation, cassandra-sharp acts mostly as a connection provider giving access to the underlying 
Cassandra.Client interface. It's pretty raw. Next versions will include:
* IDictionary wrapping
* recovery of banned endpoints

To compile, use a command line and run the following commands:
GenerateVersion.bat <VersionNumber>

You will find a package named "cassandra-sharp-bin-<VersionNumber>.zip" in the root folder.

This projects is using the following components:
* MSBuild Community Tasks Project (http://msbuildtasks.tigris.org/)
  released under BSD License (http://opensource.org/licenses/bsd-license.php)
* Apache Thrift (http://thrift.apache.org/)
  released under Apache License 2.0
* 2 classes from FluentCassandra (http://coderjournal.com/2010/04/creating-a-time-uuid-guid-in-net/)
  GuidGenerator.cs (https://github.com/managedfusion/fluentcassandra/blob/master/src/GuidGenerator.cs)
  GuidVersion.cs (https://github.com/managedfusion/fluentcassandra/blob/master/src/GuidVersion.cs)
  released under Apache License 2.0
* ILMerge from Microsoft
  released under Microsoft ILMerge EULA
* Apache Cassandra Thrift interface
  released under Apache License 2.0
* Moq (http://code.google.com/p/moq/)
  released under New BSD License
* NUnit (http://www.nunit.org/)
  released NUnit license