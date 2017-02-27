cassandra-sharp - high performance .NET driver for Apache Cassandra
===================================================================
![build status](https://ci.appveyor.com/api/projects/status/github/pchalamet/cassandra-sharp?branch=master)

The philosophy of cassandra-sharp is to be really simple and fast: no Linq provider, no complex API. Just CQL, simple object mapping and great performance :)

Starting from version 2, only CQL 3 binary protocol is supported and as a consequence, Cassandra 1.2+ is required. Also, only .NET 4.0+ is supported. If you are looking for a Thrift compatible driver, or have to use Cassandra 1.0/1.1 or require .NET 3.5 support, please consider using version 0.6.4 of cassandra-sharp.

cassandra-sharp supports async operations exposed as Rx subscriptions or TPL tasks. Efficient memory usage can be achieve using the push model of Rx.

A command line tool is also available (cqlplus) to access a Cassandra cluster. It's also a great tool to understand what's happening under the cover.

Getting binaries
================
Binaries are available through NuGet : http://www.nuget.org/packages/cassandra-sharp

Zip archive are also available at Google Code (since GitHub removed binaries uploads) : http://code.google.com/p/cassandra-sharp/downloads/list

Copyright & License
===================
	cassandra-sharp - high performance .NET driver for Apache Cassandra
	Copyright (c) 2011-2013 Pierre Chalamet

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
* TPL Task (compatible with C# 5 async) for future operations
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
To build cassandra-sharp, load cassandra-sharp.sln in Visual Studio 2012.
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
    public bool DurableWrites { get; set; }
    public string KeyspaceName { get; set; }
    public string StrategyClass { get; set; }
    public string StrategyOptions { get; set; }
}
	
public static class Sample
{
    private static void DisplayKeyspace(SchemaKeyspaces ks)
    {
        Console.WriteLine("DurableWrites={0} KeyspaceName={1} strategy_Class={2} strategy_options={3}",
                          ks.DurableWrites,
                          ks.KeyspaceName,
                          ks.StrategyClass,
                          ks.StrategyOptions);
    }
	
    public static async Task QueryKeyspaces()
    {
        XmlConfigurator.Configure();
        using (ICluster cluster = ClusterManager.GetCluster("TestCassandra"))
        {
            var cmd = cluster.CreatePocoCommand();
            const string cqlKeyspaces = "SELECT * from system.schema_keyspaces";

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
