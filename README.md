cassandra-sharp: the high performance CQL Binary Protocol for Apache Cassandra
==============================================================================
The philosophy of cassandra-sharp is to be really simple: no Linq provider, no complex API. Just CQL, simple object mapping and great performance :)
Key points of cassandra-sharp are simplicity, robustness, efficiency and thread safety.

Starting from version 2, only CQL binary protocol is supported and as a consequence, Cassandra 1.2+ is required. Also, only .NET 4.0 and 4.5 are supported.
cassandra-sharp support async operations exposed as Rx subscription or TPL tasks. Efficient memory usage can be achieve using the push model of Rx.

A command line tool is also available (cqlplus) to access a Cassandra cluster. It's also a great tool to understand what's happening under the cover.

Features
========
* async operations (TPL tasks / Rx subscriptions)
* Rx interface (IObservable / IObserver) for result streaming
* TPL Task (compatible with C# 5 async) for future operations
* Linq friendly
* extensible rowset mapping (poco and map provided out of the box)
* blazing fast object marshaler (dynamic gen'ed code)
* robust connection handling (connection recovery supported)
* multiple extension points
* command line tool (cqlplus)
* .NET 4.0/4.5 support

If you are looking for a Thrift compatible client, or have to use Cassandra 1.0/1.1 or require .NET 3.5 support, please consider using version 0.6.4 of cassandra-sharp.

Getting binaries
================
Binaries are available through NuGet : http://www.nuget.org/packages/cassandra-sharp

Zip archive are also available at Google Code (since GitHub removed binaries uploads) : http://code.google.com/p/cassandra-sharp/downloads/list

How to build
============
To build cassandra-sharp, run this command: 

	GenerateVersion.cmd

If you want to build a specific version or specific flavor, use:

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
                cmd.Execute<SchemaKeyspaces>(cqlKeyspaces).Subscribe(DisplayKeyspace);

                // future
                var kss = await cmd.Execute<SchemaKeyspaces>(cqlKeyspaces).AsFuture();
                foreach (var ks in kss)
                {
                    DisplayKeyspace(ks);
                }
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