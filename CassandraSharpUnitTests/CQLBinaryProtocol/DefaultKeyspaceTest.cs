// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2018 Pierre Chalamet
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

using System;
using System.Collections.Generic;
using System.Linq;
using CassandraSharp;
using CassandraSharp.Config;
using CassandraSharp.CQLCommand;
using CassandraSharp.CQLOrdinal;
using CassandraSharp.CQLPoco;
using NUnit.Framework;

namespace CassandraSharpUnitTests.CQLBinaryProtocol
{
    public class DefaultKeyspaceTest
    {
        private const string TestKeyspace = "YouDontHaveIt_12345678";
        private ICluster cluster;

        private IClusterManager clusterManager;


        [SetUp]
        public void SetUp()
        {
            var cassandraSharpConfig = new CassandraSharpConfig();
            clusterManager = new ClusterManager(cassandraSharpConfig);
            var config = new ClusterConfig
                         {
                             Endpoints = new EndpointsConfig {Servers = new[] {"cassandra1"}},
                             DefaultKeyspace =
                                 new KeyspaceConfig
                                 {
                                     Name = TestKeyspace,
                                     DurableWrites = false,
                                     Replication = new ReplicationConfig
                                                   {
                                                       Options = new Dictionary<string, string>
                                                                 {
                                                                     {"class", "SimpleStrategy"},
                                                                     {"replication_factor", "1"}
                                                                 }
                                                   }
                                 }
                         };

            cluster = clusterManager.GetCluster(config);
        }

        [TearDown]
        public void TearDown()
        {
            var cmd = cluster.CreateCommand().FromOrdinal().ToPoco().Build();

            try
            {
                cmd.Execute("drop keyspace " + TestKeyspace).AsFuture().Wait();
            }
            catch
            {
            }

            clusterManager.Dispose();
        }

        [Test]
        public void KeyspaceCreated()
        {
            var cmd = cluster.CreatePocoCommand();

            var res = cmd.Execute<SchemaKeyspace>("SELECT * FROM system_schema.keyspaces").AsFuture().Result;
            var testKeyspace =
                res.FirstOrDefault(
                                   x => x.KeyspaceName.Equals(TestKeyspace, StringComparison.InvariantCultureIgnoreCase));

            Assert.IsNotNull(testKeyspace);
            Assert.IsFalse(testKeyspace.DurableWrites);
            Assert.AreEqual("org.apache.cassandra.locator.SimpleStrategy", testKeyspace.Replication["class"]);
            Assert.AreEqual("1", testKeyspace.Replication["replication_factor"]);
        }

        [Test]
        public void CanOmitDefaultKeyspaceName()
        {
            var cmd = cluster.CreateOrdinalCommand();

            cmd.Execute("CREATE TABLE Test (cAscii ascii,cInt int,PRIMARY KEY (cInt))").AsFuture().Wait();
            cmd.Execute("INSERT INTO Test (cAscii,cInt) VALUES ('test',1)").AsFuture().Wait();
            var res = cmd.Execute<object[]>("SELECT cAscii FROM Test WHERE cInt = 1").AsFuture().Result.Single();
            Assert.AreEqual("test", res[0]);

            res =
                cmd.Execute<object[]>(string.Format("SELECT cAscii FROM {0}.Test WHERE cInt = 1", TestKeyspace)).AsFuture().Result.Single();
            Assert.AreEqual("test", res[0]);
        }

        private class SchemaKeyspace
        {
            public string KeyspaceName { get; set; }

            public bool DurableWrites { get; set; }

            public Dictionary<string, string> Replication { get; set; }
        }
    }
}