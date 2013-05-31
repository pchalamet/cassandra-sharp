

namespace CassandraSharpUnitTests.EndpointStrategy
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;
    using NUnit.Framework;

    [TestFixture]
    class RoundRobinFailoverEndpointStrategyTest
    {
        IEndpointStrategy _endpointStrategy;
        List<Peer> _endpoints;

        [SetUp]
        public void Init()
        {
            _endpoints = new List<Peer>()
                {
                    new Peer(IPAddress.Parse("192.168.1.1"), "DC1", "RAC1", null ),
                    new Peer(IPAddress.Parse("192.168.1.2"), "DC1", "RAC1", null ),
                    new Peer(IPAddress.Parse("192.168.2.1"), "DC1", "RAC2", null ),
                    new Peer(IPAddress.Parse("192.168.2.2"), "DC1", "RAC2", null ),
                    new Peer(IPAddress.Parse("172.168.1.1"), "DC2", "RAC1", null ),
                    new Peer(IPAddress.Parse("172.168.1.2"), "DC2", "RAC1", null )
                };
            IEndpointSnitch snitch = ServiceActivator<CassandraSharp.Snitch.Factory>
                .Create<IEndpointSnitch>("Discovery", "DC1");

            _endpointStrategy = ServiceActivator<CassandraSharp.EndpointStrategy.Factory>
                .Create<IEndpointStrategy>("RoundRobinFailover", _endpoints.Select(endpoint => endpoint.RpcAddress), snitch);

            //simulate system.peers Discovery
            foreach (var peer in _endpoints)
            {
                snitch.Update(NotificationKind.Update, peer);
                _endpointStrategy.Update(NotificationKind.Update, peer);
            }
        }

        [Test]
        public void TestNormalOperation()
        {
            var dc1Endpoints = _endpoints.Where(peer => peer.Datacenter.Equals("DC1"));
            var dc1Ips = dc1Endpoints.Select(peer => peer.RpcAddress);

            IPAddress lastIp = IPAddress.Any;
            for (int i = 0; i < 10; i++)
            {
                var picked = _endpointStrategy.Pick();

                //ip is in dc1
                Assert.IsTrue(dc1Ips.Contains(picked));
                //different ip each time
                Assert.AreNotEqual(lastIp, picked);

                lastIp = picked;
            }
        }

        [Test]
        public void TestRemoveFailover()
        {
            var dc1Endpoints = _endpoints.Where(peer => peer.Datacenter.Equals("DC1"));
            var dc1Ips = dc1Endpoints.Select(peer => peer.RpcAddress);

            var dc2Endpoints = _endpoints.Where(peer => peer.Datacenter.Equals("DC2"));
            var dc2Ips = dc2Endpoints.Select(peer => peer.RpcAddress);

            dc1Endpoints.ToList().ForEach(peer => _endpointStrategy.Update(NotificationKind.Remove, peer));

            IPAddress lastIp = IPAddress.Any;
            for (int i = 0; i < 10; i++)
            {
                var picked = _endpointStrategy.Pick();

                //ip is in dc1
                Assert.IsTrue(dc2Ips.Contains(picked));
                //different ip each time
                Assert.AreNotEqual(lastIp, picked);

                lastIp = picked;
            }

            //bring dc1 back
            _endpointStrategy.Update(NotificationKind.Add, dc1Endpoints.First());
            _endpointStrategy.Update(NotificationKind.Add, dc1Endpoints.Last());
            lastIp = IPAddress.Any;
            for (int i = 0; i < 10; i++)
            {
                var picked = _endpointStrategy.Pick();

                //ip is in dc1
                Assert.IsTrue(dc1Ips.Contains(picked));
                //different ip each time
                Assert.AreNotEqual(lastIp, picked);

                lastIp = picked;
            }
        }



        [Test]
        public void TestBanFailover()
        {
            var dc1Endpoints = _endpoints.Where(peer => peer.Datacenter.Equals("DC1"));
            var dc1Ips = dc1Endpoints.Select(peer => peer.RpcAddress);

            var dc2Endpoints = _endpoints.Where(peer => peer.Datacenter.Equals("DC2"));
            var dc2Ips = dc2Endpoints.Select(peer => peer.RpcAddress);

            dc1Endpoints.ToList().ForEach(peer => _endpointStrategy.Ban(peer.RpcAddress));

            IPAddress lastIp = IPAddress.Any;
            for (int i = 0; i < 10; i++)
            {
                var picked = _endpointStrategy.Pick();

                //ip is in dc1
                Assert.IsTrue(dc2Ips.Contains(picked));
                //different ip each time
                Assert.AreNotEqual(lastIp, picked);

                lastIp = picked;
            }

            //bring dc1 back
            _endpointStrategy.Permit(dc1Ips.First());
            _endpointStrategy.Permit(dc1Ips.Last());
            lastIp = IPAddress.Any;
            for (int i = 0; i < 10; i++)
            {
                var picked = _endpointStrategy.Pick();

                //ip is in dc1
                Assert.IsTrue(dc1Ips.Contains(picked));
                //different ip each time
                Assert.AreNotEqual(lastIp, picked);

                lastIp = picked;
            }
        }
    }
}
