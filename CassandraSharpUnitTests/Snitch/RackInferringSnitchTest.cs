// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2013 Pierre Chalamet
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

using CassandraSharp.Snitch;

namespace CassandraSharpUnitTests.Snitch
{
    using System.Net;
    using NUnit.Framework;

    [TestFixture]
    public class RackInferringSnitchTest
    {
        [Test]
        public void TestDatacenter()
        {
            IPAddress address = new IPAddress(new byte[] {192, 168, 255, 0});
            IPAddress a1 = new IPAddress(new byte[] {192, 168, 0, 0});
            IPAddress a2 = new IPAddress(new byte[] {192, 169, 10, 0});

            RackInferringSnitch rackInferringSnitch = new RackInferringSnitch();

            string adressDatacenter = rackInferringSnitch.GetDatacenter(address);
            string a1Datacenter = rackInferringSnitch.GetDatacenter(a1);
            string a2Datacenter = rackInferringSnitch.GetDatacenter(a2);

            Assert.AreEqual(adressDatacenter, a1Datacenter);
            Assert.AreNotEqual(adressDatacenter, a2Datacenter);
        }

        [Test]
        public void TestNearestEndpoint()
        {
            IPAddress address = new IPAddress(new byte[] {192, 168, 100, 0});
            IPAddress a1 = new IPAddress(new byte[] {192, 168, 0, 0});
            IPAddress a2 = new IPAddress(new byte[] {192, 168, 100, 0});

            RackInferringSnitch rackInferringSnitch = new RackInferringSnitch();

            // a2 is nearest of address
            int res = rackInferringSnitch.CompareEndpoints(address, a1, a2);
            Assert.AreEqual(1, res);

            // same distance
            res = rackInferringSnitch.CompareEndpoints(address, a1, a1);
            Assert.AreEqual(0, res);

            // a2 si nereast of address
            res = rackInferringSnitch.CompareEndpoints(address, a2, a1);
            Assert.AreEqual(-1, res);
        }

        [Test]
        public void TestRack()
        {
            IPAddress address = new IPAddress(new byte[] {192, 168, 255, 0});
            IPAddress a1 = new IPAddress(new byte[] {192, 168, 0, 0});
            IPAddress a2 = new IPAddress(new byte[] {192, 169, 255, 0});

            RackInferringSnitch rackInferringSnitch = new RackInferringSnitch();

            string adressRack = rackInferringSnitch.GetRack(address);
            string a1Rack = rackInferringSnitch.GetRack(a1);
            string a2Rack = rackInferringSnitch.GetRack(a2);

            Assert.AreEqual(adressRack, a2Rack);
            Assert.AreNotEqual(adressRack, a1Rack);
        }
    }
}