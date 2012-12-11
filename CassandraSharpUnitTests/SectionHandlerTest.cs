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

namespace CassandraSharpUnitTests
{
    using System.Configuration;
    using CassandraSharp;
    using CassandraSharp.Config;
    using NUnit.Framework;

    [TestFixture]
    public class SectionHandlerTest
    {
        [Test]
        public void TestLoadConfig()
        {
            CassandraSharpConfig cassandraSharpConfig = (CassandraSharpConfig) ConfigurationManager.GetSection("CassandraSharp");

            Assert.IsNotNull(cassandraSharpConfig);
            Assert.AreEqual("TestClient.Logger4Log4Net, TestClient", cassandraSharpConfig.Logger);
            Assert.AreEqual(4, cassandraSharpConfig.Clusters.Length);
            Assert.IsNotNull(cassandraSharpConfig.Clusters[1]);
//            Assert.IsNotNull(cassandraSharpConfig.Clusters[1].BehaviorConfig);
            Assert.IsNotNull(cassandraSharpConfig.Clusters[1].Endpoints);
            Assert.IsNotNull(cassandraSharpConfig.Clusters[1].Transport);
            Assert.AreEqual("CustomEndpointStrategy", cassandraSharpConfig.Clusters[1].Name);
            //Assert.AreEqual(3, cassandraSharpConfig.Clusters[1].BehaviorConfig.MaxRetries);
            //Assert.AreEqual("TestKeyspace", cassandraSharpConfig.Clusters[1].BehaviorConfig.KeySpace);
            //Assert.AreEqual(ConsistencyLevel.ONE, cassandraSharpConfig.Clusters[1].BehaviorConfig.DefaultReadCL);
            //Assert.AreEqual(ConsistencyLevel.QUORUM, cassandraSharpConfig.Clusters[1].BehaviorConfig.DefaultWriteCL);
            //Assert.AreEqual(null, cassandraSharpConfig.Clusters[1].BehaviorConfig.DefaultTTL);
        }
    }
}