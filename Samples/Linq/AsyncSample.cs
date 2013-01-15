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

namespace Samples.Linq
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using CassandraSharp;
    using CassandraSharp.CQLPoco;
    using CassandraSharp.Config;

    public class SchemaColumns
    {
        public string KeyspaceName { get; set; }

        public string ColumnFamilyName { get; set; }

        public string ColumnName { get; set; }

        public int ComponentIndex { get; set; }

        public string Validator { get; set; }
    }

    public static class LinqSample
    {
        public static void Run()
        {
            XmlConfigurator.Configure();
            using (ICluster cluster = ClusterManager.GetCluster("TestCassandra"))
            {
                const string cqlKeyspaces = "SELECT * from system.schema_columns";

                var req = from t in cluster.Execute<SchemaColumns>(cqlKeyspaces).Result
                          where t.KeyspaceName == "system"
                          select t;
                DisplayResult(req);
            }

            ClusterManager.Shutdown();
        }

        private static void DisplayResult(IEnumerable<SchemaColumns> req)
        {
            foreach (SchemaColumns schemaColumns in req)
            {
                Console.WriteLine("KeyspaceName={0} ColumnFamilyName={1} ColumnName={2}",
                                  schemaColumns.KeyspaceName, schemaColumns.ColumnFamilyName, schemaColumns.ColumnName);
            }
        }
    }
}