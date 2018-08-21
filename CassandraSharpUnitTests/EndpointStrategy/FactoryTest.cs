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

using CassandraSharp.EndpointStrategy;
using CassandraSharp.Snitch;
using CassandraSharp.Utils;

namespace CassandraSharpUnitTests.EndpointStrategy
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Numerics;
    using CassandraSharp.Extensibility;
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

            public IPAddress Pick(BigInteger? token)
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

            public void Update(NotificationKind kind, Peer peer)
            {
                throw new NotImplementedException();
            }
        }

        [Test]
        public void TestCreateCustom()
        {
            string customType = typeof(CustomEndpointStrategy).AssemblyQualifiedName;

            IEnumerable<IPAddress> endpoints = new List<IPAddress> {null};
            IEndpointStrategy endpointStrategy = ServiceActivator<CassandraSharp.EndpointStrategy.Factory>.Create<IEndpointStrategy>(customType, endpoints,
                                                                                                                                     new SimpleSnitch());

            CustomEndpointStrategy customEndpointStrategy = endpointStrategy as CustomEndpointStrategy;
            Assert.IsNotNull(customEndpointStrategy);
            IPAddress customEndpoint = customEndpointStrategy.Endpoints.Single();
            Assert.AreEqual(customEndpoint, endpoints.Single());
        }

        [Test]
        public void TestCreateNearest()
        {
            IEndpointStrategy endpointStrategy = ServiceActivator<CassandraSharp.EndpointStrategy.Factory>.Create<IEndpointStrategy>("Nearest",
                                                                                                                                     Enumerable.Empty<IPAddress>
                                                                                                                                             (),
                                                                                                                                     new SimpleSnitch());
            Assert.IsTrue(endpointStrategy is NearestEndpointStrategy);
        }

        [Test]
        public void TestCreateRandom()
        {
            IEndpointStrategy endpointStrategy = ServiceActivator<CassandraSharp.EndpointStrategy.Factory>.Create<IEndpointStrategy>("Random",
                                                                                                                                     Enumerable.Empty<IPAddress>
                                                                                                                                             (),
                                                                                                                                     new SimpleSnitch());
            Assert.IsTrue(endpointStrategy is RandomEndpointStrategy);
        }
    }
}