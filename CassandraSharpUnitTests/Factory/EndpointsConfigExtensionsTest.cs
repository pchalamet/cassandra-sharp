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

            IEndpointStrategy endpointStrategy = config.Create("CassandraSharpUnitTests.Factory.CustomEndpointStrategy, CassandraSharpUnitTests",
                                                               Enumerable.Empty<Endpoint>());
            Assert.IsTrue(endpointStrategy is CustomEndpointStrategy);
        }

        [Test]
        public void TestCreateNearest()
        {
            EndpointsConfig config = new EndpointsConfig();
            config.Strategy = EndpointStrategy.Nearest;

            IEndpointStrategy endpointStrategy = config.Create(null, Enumerable.Empty<Endpoint>());
            Assert.IsTrue(endpointStrategy is NearestEndpointStrategy);
        }

        [Test]
        public void TestCreateRandom()
        {
            EndpointsConfig config = new EndpointsConfig();
            config.Strategy = EndpointStrategy.Random;

            IEndpointStrategy endpointStrategy = config.Create(null, Enumerable.Empty<Endpoint>());
            Assert.IsTrue(endpointStrategy is RandomEndpointStrategy);
        }
    }
}