
namespace CassandraSharpUnitTests.Snitch
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Snitch;
    using CassandraSharp.Utils;
    using NUnit.Framework;


    [TestFixture]
    public class DiscoverySnitchTest
    {
        private IEndpointSnitch _snitch;
        private List<Peer> _endpoints;

        [SetUp]
        public void Init()
        {
            //must be ordered by proximity to first element to pass TestSortedByProximity()
            _endpoints = new List<Peer>()
                {
                    new Peer(IPAddress.Parse("192.168.1.1"), "DC1", "RAC1", null ),
                    new Peer(IPAddress.Parse("192.168.1.2"), "DC1", "RAC1", null ),
                    new Peer(IPAddress.Parse("192.168.2.1"), "DC1", "RAC2", null ),
                    new Peer(IPAddress.Parse("192.168.2.2"), "DC1", "RAC2", null ),
                    new Peer(IPAddress.Parse("172.168.1.1"), "DC2", "RAC1", null ),
                    new Peer(IPAddress.Parse("172.168.1.2"), "DC2", "RAC1", null )
                };
            _snitch = ServiceActivator<Factory>.Create<IEndpointSnitch>("Discovery");
            _endpoints.ForEach(peer => _snitch.Update(NotificationKind.Update, peer));
        }

        [Test]
        public void TestDatacenter()
        {
            string adressDatacenter = _snitch.GetDatacenter(_endpoints[0].RpcAddress);
            string a1Datacenter = _snitch.GetDatacenter(_endpoints[2].RpcAddress);
            string a2Datacenter = _snitch.GetDatacenter(_endpoints[4].RpcAddress);

            Assert.AreEqual(adressDatacenter, a1Datacenter);
            Assert.AreNotEqual(adressDatacenter, a2Datacenter);
        }


        [Test]
        public void TestNearestEndpoint()
        {
            IPAddress address = _endpoints[1].RpcAddress;
            IPAddress a1 = _endpoints[5].RpcAddress;
            IPAddress a2 = _endpoints[3].RpcAddress;

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
            string adressRack = _snitch.GetRack(_endpoints[0].RpcAddress);
            string a1Rack = _snitch.GetRack(_endpoints[1].RpcAddress);
            string a2Rack = _snitch.GetRack(_endpoints[3].RpcAddress);

            Assert.AreEqual(adressRack, a1Rack);
            Assert.AreNotEqual(adressRack, a2Rack);
        }


        [Test]
        public void TestSortedByProximity()
        {
            Random rand = new Random();

            //shuffle list
            var res = _snitch.GetSortedListByProximity(_endpoints[0].RpcAddress,
                _endpoints.Select(endpoint => endpoint.RpcAddress)
                .OrderBy(endpoint => rand.Next()));

            //check if ordering has been restored by dc & rack
            //(ip will be randonly distributed within rack/dc groups)
            for (int i = 0; i < res.Count; i++)
            {
                Assert.AreEqual(_endpoints[i].Datacenter, _snitch.GetDatacenter(res[i]));
                Assert.AreEqual(_endpoints[i].Rack, _snitch.GetRack(res[i]));
            }
        }
    }
}
