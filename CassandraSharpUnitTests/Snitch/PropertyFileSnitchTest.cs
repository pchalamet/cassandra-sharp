

namespace CassandraSharpUnitTests.Snitch
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Net;
    using CassandraSharp.Snitch;
    using NUnit.Framework;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;

    [TestFixture]
    class PropertyFileSnitchTest
    {
        private IEndpointSnitch _snitch;
        private List<Tuple<IPAddress, string, string>> _endpoints;

        [SetUp]
        public void Init()
        {
            //must be ordered by proximity to first element to pass TestSortedByProximity()
            _endpoints = new List<Tuple<IPAddress, string, string>>()
                {
                    Tuple.Create(IPAddress.Parse("192.168.1.1"), "DC1", "RAC1"),
                    Tuple.Create(IPAddress.Parse("192.168.1.2"), "DC1", "RAC1"),
                    Tuple.Create(IPAddress.Parse("192.168.2.1"), "DC1", "RAC2"),
                    Tuple.Create(IPAddress.Parse("192.168.2.2"), "DC1", "RAC2"),
                    Tuple.Create(IPAddress.Parse("172.168.1.1"), "DC2", "RAC1"),
                    Tuple.Create(IPAddress.Parse("172.168.1.2"), "DC2", "RAC1")
                };
            _snitch = ServiceActivator<Factory>.Create<IEndpointSnitch>("PropertyFile", _endpoints);
        }

        [Test]
        public void TestDatacenter()
        {
            string adressDatacenter = _snitch.GetDatacenter(_endpoints[0].Item1);
            string a1Datacenter = _snitch.GetDatacenter(_endpoints[2].Item1);
            string a2Datacenter = _snitch.GetDatacenter(_endpoints[4].Item1);

            Assert.AreEqual(adressDatacenter, a1Datacenter);
            Assert.AreNotEqual(adressDatacenter, a2Datacenter);
        }

        [Test]
        public void TestNearestEndpoint()
        {
            IPAddress address = _endpoints[1].Item1;
            IPAddress a1 = _endpoints[5].Item1;
            IPAddress a2 = _endpoints[3].Item1;

            // a2 is nearest of address
            int res = _snitch.CompareEndpoints(address, a1, a2);
            Assert.AreEqual(1, res);

            // same distance
            res = _snitch.CompareEndpoints(address, a1, a1);
            Assert.AreEqual(0, res);

            // a2 si nereast of address
            res = _snitch.CompareEndpoints(address, a2, a1);
            Assert.AreEqual(-1, res);
        }



        [Test]
        public void TestRack()
        {
            string adressRack = _snitch.GetRack(_endpoints[0].Item1);
            string a1Rack = _snitch.GetRack(_endpoints[1].Item1);
            string a2Rack = _snitch.GetRack(_endpoints[3].Item1);

            Assert.AreEqual(adressRack, a1Rack);
            Assert.AreNotEqual(adressRack, a2Rack);
        }


        [Test]
        public void TestSortedByProximity()
        {
            Random rand = new Random();

            //shuffle list
            var res = _snitch.GetSortedListByProximity(_endpoints[0].Item1,
                _endpoints.Select(endpoint => endpoint.Item1)
                .OrderBy(endpoint => rand.Next()));

            //check if ordering has been restored by dc & rack
            //(ip will be randonly distributed within rack/dc groups)
            for (int i = 0; i < res.Count; i++)
            {
                Assert.AreEqual(_endpoints[i].Item2, _snitch.GetDatacenter(res[i]));
                Assert.AreEqual(_endpoints[i].Item3, _snitch.GetRack(res[i]));
            }
        }
    }
}
