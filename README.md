cassandra-sharp - high performance .NET driver for Apache Cassandra
===================================================================
![build status](https://ci.appveyor.com/api/projects/status/github/pchalamet/cassandra-sharp?branch=master)

The philosophy of cassandra-sharp is to be really simple and fast: no Linq provider, no complex API. Just CQL, simple object mapping and great performance :)

With version 4, cassandra-sharp is only speaking native protocol 4 - basically, this means you are required to use Cassandra 3.

cassandra-sharp supports async operations exposed as Rx subscriptions or TPL tasks. Efficient memory usage can be achieve using the push model of Rx.

A command line tool is also available (cqlplus) to access a Cassandra cluster. It's also a great tool to understand what's happening under the cover.

Getting binaries
================
Binaries are available through NuGet : http://www.nuget.org/packages/cassandra-sharp

Copyright & License
===================
	cassandra-sharp - high performance .NET driver for Apache Cassandra
	Copyright (c) 2011-2017 Pierre Chalamet

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	 
	http://www.apache.org/licenses/LICENSE-2.0
	 
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.

Features
========
* async operations (TPL tasks / Rx subscriptions)
* Rx interface (IObservable / IObserver) for result streaming
* TPL Task for future operations
* Linq friendly
* extensible rowset mapping (poco and map provided out of the box)
* blazing fast object marshaler (dynamic gen'ed code)
* robust connection handling (connection recovery supported)
* ability to understand performance issues (client and server side)
* multiple extension points
* command line tool (cqlplus)
* .NET 4.0+ support (Microsoft .NET / Mono)

How to build
============
To build cassandra-sharp, load cassandra-sharp.sln in Visual Studio 2017.
To build from command line and to regenerate thrift proxy, use Build.cmd.

Sample configuration
====================
```xml
<configSections>
	<section name="CassandraSharp" type="CassandraSharp.SectionHandler, CassandraSharp.Interfaces" />
</configSections>

<CassandraSharp>
	<Cluster name="TestCassandra">
		<Endpoints>
			<Server>localhost</Server>
		</Endpoints>
	</Cluster>
</CassandraSharp>
```
Sample client
=============
```c#
public class SchemaKeyspaces
{
    public string KeyspaceName { get; set; }
    public bool DurableWrites { get; set; }
    public Dictionary<string, string> Replication { get; set; }
}
	
public static class Sample
{
    private static void DisplayKeyspace(SchemaKeyspaces ks)
    {
        Console.WriteLine("KeyspaceName={0} DurableWrites={1} Class={2} ReplicationFactor={3}",
                          ks.KeyspaceName,
                          ks.DurableWrites,
                          ks.Replication["class"],
                          ks.Replication["replication_factor"]);
    }
	
    public static async Task QueryKeyspaces()
    {
        XmlConfigurator.Configure();
        using (ICluster cluster = ClusterManager.GetCluster("TestCassandra"))
        {
            var cmd = cluster.CreatePocoCommand();
            const string cqlKeyspaces = "SELECT * from system_schema.keyspaces";

            // async operation with streaming
            cmd.WithConsistencyLevel(ConsistencyLevel.ONE)
               .Execute<SchemaKeyspaces>(cqlKeyspaces)
               .Subscribe(DisplayKeyspace);

            // future
            var kss = await cmd.Execute<SchemaKeyspaces>(cqlKeyspaces).AsFuture();
            foreach (var ks in kss)
                DisplayKeyspace(ks);
        }

        ClusterManager.Shutdown();
    }
}
```
Thanks
======
JetBrains provided a free licence of Resharper for the cassandra-sharp project. Big thanks for the awesome product.
![ReSharper](http://www.jetbrains.com/resharper/features/rs/rs1/rs468x60_violet.gif)

This projects also relies on the following third parties:
* MSBuild Community Tasks Project (http://msbuildtasks.tigris.org/) released under BSD License
* Moq (http://code.google.com/p/moq/) released under New BSD License
* NUnit (http://www.nunit.org/) released NUnit license
* NuGet (http://nuget.org/) released under Apache License 2.0
* Command Line Argument Parser Library (http://commandlinearguments.codeplex.com/) released under Microsoft Public License (Ms-PL)
* Tiny Parser Generator (http://www.codeproject.com/Articles/28294/a-Tiny-Parser-Generator-v1-2) released under Code Project Open License (CPOL)

Thanks to all contributors for ideas, bug fix and feedbacks!

[![githalytics.com alpha](https://cruel-carlota.pagodabox.com/8727d7a4294e4c1821f74094438ca26d "githalytics.com")](http://githalytics.com/pchalamet/cassandra-sharp)
