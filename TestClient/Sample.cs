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

namespace TestClient
{
    using System;
    using System.Collections.Generic;
    using Apache.Cassandra;
    using CassandraSharp;

    public abstract class Sample
    {
        private readonly string _configName;

        private readonly string _keyspace;

        protected Sample(string keyspace, string configName)
        {
            _keyspace = keyspace;
            _configName = configName;
        }

        public void Run()
        {
            Console.WriteLine("===========================================================================");
            Console.WriteLine("Running test {0}", GetType().Name);

            try
            {
                CreateKeyspace();
                using (ICluster cluster = ClusterManager.GetCluster(_configName))
                {
                    CreateSchema(cluster);
                    RunSample(cluster);
                    DropSchema(cluster);
                }
                DropKeyspace();

                Console.WriteLine();
                Console.WriteLine("TEST COMPLETED SUCCESSFULLY");
                Console.WriteLine("===========================================================================");
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine("TEST FAILED WITH ERROR:");
                Console.WriteLine(ex);
                Console.WriteLine("===========================================================================");
            }
            Console.WriteLine();
            Console.WriteLine();
        }

        protected abstract void RunSample(ICluster cluster);

        protected virtual void CreateKeyspace()
        {
            using (ICluster cluster = ClusterManager.GetCluster("MinimalConfig"))
                cluster.Execute(x => RecreateKeyspace(x.CassandraClient));
        }

        protected virtual void DropKeyspace()
        {
            using (ICluster cluster = ClusterManager.GetCluster("MinimalConfig"))
                cluster.Execute(x => x.CassandraClient.system_drop_keyspace(_keyspace));
        }

        private void RecreateKeyspace(Cassandra.Client client)
        {
            try
            {
                client.system_drop_keyspace(_keyspace);
            }
// ReSharper disable EmptyGeneralCatchClause
            catch
// ReSharper restore EmptyGeneralCatchClause
            {
            }

            KsDef ksDef = new KsDef
                              {
                                  Name = _keyspace,
                                  Strategy_class = "SimpleStrategy",
                                  Cf_defs = new List<CfDef>(),
                                  Strategy_options = new Dictionary<string, string>()
                              };
            ksDef.Strategy_options["replication_factor"] = "1";

            client.system_add_keyspace(ksDef);
        }

        protected virtual void CreateSchema(ICluster cluster)
        {
        }

        protected virtual void DropSchema(ICluster cluster)
        {
        }
    }
}