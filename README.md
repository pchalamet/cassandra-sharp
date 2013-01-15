cassandra-sharp
===============
cassandra-sharp is a .NET client for Apache Cassandra.  The philosophy of cassandra-sharp is to be really simple: no Linq provider, no complex API. Just CQL, simple object mapping and great performance :)

Starting from version 2, only CQL binary protocol is supported and as a consequence, Cassandra 1.2+ is required. Version 2 is also only .NET 4.0+ compatible.
cassandra-sharp support async operations exposed as TPL tasks and efficient memory usage (streaming as much as it can using a pull model). Futures are also supported as well. Key points of cassandra-sharp are simplicity, robustness, efficiency, thread safety and cross-platform (Microsoft .NET or Mono).

A command line tool is also available (cqlplus) to access a Cassandra cluster. It's also a great tool to understand what's happening under the cover.

Features
========
* async operations
* TPL integration (compatible with C# 5 async)
* streaming support with IEnumerable (compatible with Linq)
* extensible rowset mapping (poco and map provided out of the box)
* robust connection handling (connection can then be recovered)
* multiple extension points
* command line tool (cqlplus)
* .NET 4+ compatible (Microsoft .NET or Mono)

If you are looking for a Thrift compatible client, or have to use Cassandra 1.0/1.1 or require .NET 3.5 support, please consider using version 0.6.4 of cassandra-sharp.

Getting binaries
================
Binaries are available through NuGet : http://www.nuget.org/packages/cassandra-sharp

Zip archive are also available at Google Code (since GitHub removed binaries uploads) : http://code.google.com/p/cassandra-sharp/downloads/list

How to build
============
To build cassandra-sharp, run this command: 

	GenerateVersion.cmd <flavor> <VersionNumber> <VersionStatus>
	
NOTE: type "GenerateVersion.cmd /?" for more info.
  
You will find a package named in the OutDir folder:
	cassandra-sharp-bin-<VersionNumber>-<VersionStatus>.zip

Sample configuration
====================
	<configSections>
		<section name="CassandraSharp" type="CassandraSharp.SectionHandler, CassandraSharp" />
	</configSections>

	<CassandraSharp>
		<Cluster name="TestCassandra">
			<Endpoints>
				<Server>localhost</Server>
			</Endpoints>
		</Cluster>
	</CassandraSharp>

Sample client
=============
	using System;
	using System.Collections.Generic;
	using CassandraSharp;
	using CassandraSharp.CQL;
	using CassandraSharp.CQLPoco;
	using CassandraSharp.Config;

	public class SchemaKeyspaces
	{
		public bool DurableWrites { get; set; }

		public string KeyspaceName { get; set; }

		public string StrategyClass { get; set; }

		public string StrategyOptions { get; set; }
	}

	public static class Sample
	{
		private static void DisplayKeyspace(IEnumerable<SchemaKeyspaces> result)
		{
			foreach (var resKeyspace in result)
			{
				Console.WriteLine("DurableWrites={0} KeyspaceName={1} strategy_Class={2} strategy_options={3}",
									resKeyspace.DurableWrites,
									resKeyspace.KeyspaceName,
									resKeyspace.StrategyClass,
									resKeyspace.StrategyOptions);
			}
		}

		public async static Task QueryKeyspaces()
		{
			XmlConfigurator.Configure();
			using (ICluster cluster = ClusterManager.GetCluster("TestCassandra"))
			{
				const string cqlKeyspaces = "SELECT * from system.schema_keyspaces";

				// async operation
				var taskKeyspaces = cluster.Execute<SchemaKeyspaces>(cqlKeyspaces);

				// future operation meanwhile
				var futKeyspaces = cluster.Execute<SchemaKeyspaces>(cqlKeyspaces)
										  .AsFuture();

				// display the result of the async operation
				var result = await taskKeyspaces;
				DisplayKeyspace(result);

				// display the future
				DisplayKeyspace(futKeyspaces.Result);
			}
			ClusterManager.Shutdown();
		}
	}

Thanks
======
JetBrains provided a free licence of Resharper for the cassandra-sharp project. Big thanks for the awesome product.

This projects also relies on the following third parties:
* MSBuild Community Tasks Project (http://msbuildtasks.tigris.org/) released under BSD License
* Moq (http://code.google.com/p/moq/) released under New BSD License
* NUnit (http://www.nunit.org/) released NUnit license
* NuGet (http://nuget.org/) released under Apache License 2.0
* Command Line Argument Parser Library (http://commandlinearguments.codeplex.com/) released under Microsoft Public License (Ms-PL)
* Tiny Parser Generator (http://www.codeproject.com/Articles/28294/a-Tiny-Parser-Generator-v1-2) released under Code Project Open License (CPOL)

[![githalytics.com alpha](https://cruel-carlota.pagodabox.com/8727d7a4294e4c1821f74094438ca26d "githalytics.com")](http://githalytics.com/pchalamet/cassandra-sharp)