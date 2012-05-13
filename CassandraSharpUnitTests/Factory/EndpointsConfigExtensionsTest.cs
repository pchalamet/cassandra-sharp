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

namespace CassandraSharpUnitTests.Factory
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using CassandraSharp.Config;
    using CassandraSharp.EndpointStrategy;
    using CassandraSharp.Factory;
    using NUnit.Framework;

    public class CustomEndpointStrategy : IEndpointStrategy
    {
        public CustomEndpointStrategy(IEnumerable<Endpoint> endpoints)
        {
        }

        public Endpoint Pick()
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

    [TestFixture]
    public class EndpointsConfigExtensionsTest
    {
        [Test]
        public void TestCreateCustom()
        {
            EndpointsConfig config = new EndpointsConfig();
            config.Strategy = EndpointStrategy.Custom;

            IEndpointStrategy endpointStrategy = EndpointsConfigFactory.Create(config,
                                                                               "CassandraSharpUnitTests.Factory.CustomEndpointStrategy, CassandraSharpUnitTests",
                                                                               Enumerable.Empty<Endpoint>());
            Assert.IsTrue(endpointStrategy is CustomEndpointStrategy);
        }

        [Test]
        public void TestCreateNearest()
        {
            EndpointsConfig config = new EndpointsConfig();
            config.Strategy = EndpointStrategy.Nearest;

            IEndpointStrategy endpointStrategy = EndpointsConfigFactory.Create(config, null, Enumerable.Empty<Endpoint>());
            Assert.IsTrue(endpointStrategy is NearestEndpointStrategy);
        }

        [Test]
        public void TestCreateRandom()
        {
            EndpointsConfig config = new EndpointsConfig();
            config.Strategy = EndpointStrategy.Random;

            IEndpointStrategy endpointStrategy = EndpointsConfigFactory.Create(config, null, Enumerable.Empty<Endpoint>());
            Assert.IsTrue(endpointStrategy is RandomEndpointStrategy);
        }
    }
}