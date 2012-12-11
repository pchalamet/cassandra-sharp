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

namespace CassandraSharpUnitTests.EndpointStrategy
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using CassandraSharp;
    using CassandraSharp.EndpointStrategy;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Snitch;
    using NUnit.Framework;

    [TestFixture]
    public class EndpointsConfigExtensionsTest
    {
// ReSharper disable ClassNeverInstantiated.Local
        private class CustomEndpointStrategy : IEndpointStrategy
// ReSharper restore ClassNeverInstantiated.Local
        {
            public CustomEndpointStrategy(IEnumerable<IPAddress> endpoints, IEndpointSnitch snitch)
            {
                Endpoints = endpoints;
            }

            public IEnumerable<IPAddress> Endpoints { get; set; }

            public IPAddress Pick(Token token)
            {
                throw new NotImplementedException();
            }

            public void Ban(IPAddress endpoint)
            {
                throw new NotImplementedException();
            }

            public void Permit(IPAddress endpoint)
            {
                throw new NotImplementedException();
            }

            public void Update(IEnumerable<IPAddress> endpoints)
            {
                throw new NotImplementedException();
            }
        }

        [Test]
        public void TestCreateCustom()
        {
            string customType = typeof(CustomEndpointStrategy).AssemblyQualifiedName;

            IEnumerable<IPAddress> endpoints = new List<IPAddress> {null};
            IEndpointStrategy endpointStrategy = CassandraSharp.EndpointStrategy.Factory.Create(customType, endpoints, new SimpleSnitch());

            CustomEndpointStrategy customEndpointStrategy = endpointStrategy as CustomEndpointStrategy;
            Assert.IsNotNull(customEndpointStrategy);
            IPAddress customEndpoint = customEndpointStrategy.Endpoints.Single();
            Assert.AreEqual(customEndpoint, endpoints.Single());
        }

        [Test]
        public void TestCreateNearest()
        {
            IEndpointStrategy endpointStrategy = CassandraSharp.EndpointStrategy.Factory.Create("Nearest", Enumerable.Empty<IPAddress>(), new SimpleSnitch());
            Assert.IsTrue(endpointStrategy is NearestEndpointStrategy);
        }

        [Test]
        public void TestCreateRandom()
        {
            IEndpointStrategy endpointStrategy = CassandraSharp.EndpointStrategy.Factory.Create("Random", Enumerable.Empty<IPAddress>(), new SimpleSnitch());
            Assert.IsTrue(endpointStrategy is RandomEndpointStrategy);
        }
    }
}