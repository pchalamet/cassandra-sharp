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

namespace CassandraSharpUnitTests.Snitch
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using CassandraSharp;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Snitch;
    using NUnit.Framework;

    [TestFixture]
    public class SnitchTypeExtensionsTest
    {
        private class CustomSnitch : IEndpointSnitch
        {
            public string GetRack(IPAddress endpoint)
            {
                throw new NotImplementedException();
            }

            public string GetDatacenter(IPAddress endpoint)
            {
                throw new NotImplementedException();
            }

            public List<IPAddress> GetSortedListByProximity(IPAddress address, IEnumerable<IPAddress> unsortedAddress)
            {
                throw new NotImplementedException();
            }

            public int CompareEndpoints(IPAddress address, IPAddress a1, IPAddress a2)
            {
                throw new NotImplementedException();
            }
        }

        [Test]
        public void TestCreateCustom()
        {
            string customType = typeof(CustomSnitch).AssemblyQualifiedName;
            IEndpointSnitch snitch = Factory.Create(customType);
            Assert.IsTrue(snitch is CustomSnitch);
        }

        [Test]
        public void TestCreateRackInferring()
        {
            IEndpointSnitch snitch = Factory.Create("RackInferring");
            Assert.IsTrue(snitch is RackInferringSnitch);
        }

        [Test]
        public void TestCreateSimple()
        {
            IEndpointSnitch snitch = Factory.Create("Simple");
            Assert.IsTrue(snitch is SimpleSnitch);
        }
    }
}