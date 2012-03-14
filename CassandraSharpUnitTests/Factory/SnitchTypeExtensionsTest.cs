namespace CassandraSharpUnitTests.Factory
{
    using System;
    using System.Net;
    using CassandraSharp.Config;
    using CassandraSharp.Factory;
    using CassandraSharp.Snitch;
    using NUnit.Framework;

    public class CustomSnitch : ISnitch
    {
        public string GetDataCenter(IPAddress target)
        {
            throw new NotImplementedException();
        }

        public int ComputeDistance(IPAddress source, IPAddress target)
        {
            throw new NotImplementedException();
        }
    }

    [TestFixture]
    public class SnitchTypeExtensionsTest
    {
        [Test]
        public void TestCreateCustom()
        {
            SnitchType snitchType = SnitchType.Custom;
            ISnitch snitch = snitchType.Create("CassandraSharpUnitTests.Factory.CustomSnitch, CassandraSharpUnitTests");
            Assert.IsTrue(snitch is CustomSnitch);
        }

        [Test]
        public void TestCreateRackInferring()
        {
            SnitchType snitchType = SnitchType.RackInferring;
            ISnitch snitch = snitchType.Create(null);
            Assert.IsTrue(snitch is RackInferringSnitch);
        }

        [Test]
        public void TestCreateSimple()
        {
            SnitchType snitchType = SnitchType.Simple;
            ISnitch snitch = snitchType.Create(null);
            Assert.IsTrue(snitch is SimpleSnitch);
        }
    }
}