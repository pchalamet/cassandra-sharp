// cassandra-sharp - a .NET client for Apache Cassandra
// Copyright (c) 2011-2012 Pierre Chalamet
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace CassandraClient.Sample
{
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
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
            var taskKeyspaces = cluster.Execute<SchemaKeyspaces>(cqlKeyspaces, ConsistencyLevel.QUORUM);

            // future operation meanwhile
            var futKeyspaces = cluster.Execute<SchemaKeyspaces>(cqlKeyspaces, ConsistencyLevel.QUORUM)
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
}