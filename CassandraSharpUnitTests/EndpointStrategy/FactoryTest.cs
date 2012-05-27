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
    using CassandraSharp;
    using CassandraSharp.EndpointStrategy;
    using NUnit.Framework;

    [TestFixture]
    public class EndpointsConfigExtensionsTest
    {
// ReSharper disable ClassNeverInstantiated.Local
        private class CustomEndpointStrategy : IEndpointStrategy
// ReSharper restore ClassNeverInstantiated.Local
        {
            public CustomEndpointStrategy(IEnumerable<Endpoint> endpoints)
            {
                Endpoints = endpoints;
            }

            public IEnumerable<Endpoint> Endpoints { get; set; }

            public Endpoint Pick(byte[] keyHint)
            {
                throw new NotImplementedException();
            }

            public void Ban(Endpoint endpoint)
            {
                throw new NotImplementedException();
            }

            public void Permit(Endpoint endpoint)
            {
                throw new NotImplementedException();
            }
        }

        [Test]
        public void TestCreateCustom()
        {
            string customType = typeof(CustomEndpointStrategy).AssemblyQualifiedName;

            IEnumerable<Endpoint> endpoints = new List<Endpoint> {new Endpoint("toto", null, "dc1", 666)};
            IEndpointStrategy endpointStrategy = Factory.Create(customType, endpoints);

            CustomEndpointStrategy customEndpointStrategy = endpointStrategy as CustomEndpointStrategy;
            Assert.IsNotNull(customEndpointStrategy);
            Endpoint customEndpoint = customEndpointStrategy.Endpoints.Single();
            Assert.AreEqual(customEndpoint, endpoints.Single());
        }

        [Test]
        public void TestCreateNearest()
        {
            IEndpointStrategy endpointStrategy = Factory.Create("Nearest", Enumerable.Empty<Endpoint>());
            Assert.IsTrue(endpointStrategy is NearestEndpointStrategy);
        }

        [Test]
        public void TestCreateRandom()
        {
            IEndpointStrategy endpointStrategy = Factory.Create("Random", Enumerable.Empty<Endpoint>());
            Assert.IsTrue(endpointStrategy is RandomEndpointStrategy);
        }
    }
}