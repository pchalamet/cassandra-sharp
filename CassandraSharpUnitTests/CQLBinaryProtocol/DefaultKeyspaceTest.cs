// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2013 Pierre Chalamet
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

namespace CassandraSharpUnitTests.CQLBinaryProtocol
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using CassandraSharp;
    using CassandraSharp.Config;
    using CassandraSharp.CQLCommand;
    using CassandraSharp.CQLOrdinal;
    using CassandraSharp.CQLPoco;
    using NUnit.Framework;

    public class DefaultKeyspaceTest
    {
        private const string TestKeyspace = "YouDontHaveIt_12345678";

        private ICluster cluster;

        [SetUp]
        public void SetUp()
        {
            CassandraSharpConfig cassandraSharpConfig = new CassandraSharpConfig();
            ClusterManager.Configure(cassandraSharpConfig);

            var config = new ClusterConfig
            {
                Endpoints = new EndpointsConfig { Servers = new[] { "localhost" } },
                DefaultKeyspace =
                    new KeyspaceConfig
                    {
                        Name = TestKeyspace,
                        DurableWrites = false,
                        Replication = new ReplicationConfig
                        {
                            Options = new Dictionary<string, string>
                                            {
                                                { "class", "SimpleStrategy" },
                                                { "replication_factor", "1" },
                                            }
                        }
                    }
            };

            cluster = ClusterManager.GetCluster(config);
        }

        [TearDown]
        public void TearDown()
        {
            ICqlCommand cmd = cluster.CreateCommand().FromOrdinal().ToPoco().Build();

            try
            {
                cmd.Execute("drop keyspace " + TestKeyspace).AsFuture().Wait();
            }
            catch
            {
            }

            ClusterManager.Shutdown();
        }

        [Test]
        public void KeyspaceCreated()
        {
            ICqlCommand cmd = cluster.CreatePocoCommand();

            var res = cmd.Execute<SchemaKeyspace>("SELECT * FROM system.schema_keyspaces").AsFuture().Result;
            var testKeyspace =
                res.FirstOrDefault(
                    x => x.KeyspaceName.Equals(DefaultKeyspaceTest.TestKeyspace, StringComparison.InvariantCultureIgnoreCase));

            Assert.IsNotNull(testKeyspace);
            Assert.IsFalse(testKeyspace.DurableWrites);
            Assert.IsTrue(testKeyspace.StrategyClass.Contains("SimpleStrategy"));
            Assert.IsTrue(testKeyspace.StrategyOptions.Contains("1"));
        }

        [Test]
        public void CanOmitDefaultKeyspaceName()
        {
            ICqlCommand cmd = cluster.CreateOrdinalCommand();
            
            cmd.Execute("CREATE TABLE Test (cAscii ascii,cInt int,PRIMARY KEY (cInt))").AsFuture().Wait();
            cmd.Execute("INSERT INTO Test (cAscii,cInt) VALUES ('test',1)").AsFuture().Wait();            
            var res = cmd.Execute<object[]>("SELECT cAscii FROM Test WHERE cInt = 1").AsFuture().Result.Single();            
            Assert.AreEqual("test", res[0]);

            res =
                cmd.Execute<object[]>(string.Format("SELECT cAscii FROM {0}.Test WHERE cInt = 1", TestKeyspace)).
                    AsFuture().Result.Single();
            Assert.AreEqual("test", res[0]);
        }

        private class SchemaKeyspace
        {
            public string KeyspaceName { get; set; }

            public bool DurableWrites { get; set; }

            public string StrategyClass { get; set; }

            public string StrategyOptions { get; set; }
        }
    }
}