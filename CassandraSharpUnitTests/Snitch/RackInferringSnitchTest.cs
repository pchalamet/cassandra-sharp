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

namespace CassandraSharpUnitTests.Snitch
{
    using System.Net;
    using CassandraSharp.Snitch;
    using NUnit.Framework;

    [TestFixture]
    public class RackInferringSnitchTest
    {
        public void TestNearestEndpoint()
        {
            IPAddress source = new IPAddress(new byte[] {192, 168, 255, 0});
            IPAddress target1 = new IPAddress(new byte[] {192, 168, 0, 0});
            IPAddress target2 = new IPAddress(new byte[] {192, 168, 100, 0});

            // it should be less costly to 
            RackInferringSnitch rackInferringSnitch = new RackInferringSnitch();
            int dist1 = rackInferringSnitch.ComputeDistance(source, target1);
            int dist2 = rackInferringSnitch.ComputeDistance(source, target2);
            Assert.IsTrue(dist2 < dist1);
        }

        [Test]
        public void TestEndpointInSameDatacenterIsNearest()
        {
            IPAddress source = new IPAddress(new byte[] {192, 168, 255, 0});
            IPAddress target_same_dc = new IPAddress(new byte[] {192, 168, 0, 0});
            IPAddress target_other_dc = new IPAddress(new byte[] {192, 169, 0, 0});

            // it should be less costly to 
            RackInferringSnitch rackInferringSnitch = new RackInferringSnitch();
            int dist1 = rackInferringSnitch.ComputeDistance(source, target_same_dc);
            int dist2 = rackInferringSnitch.ComputeDistance(source, target_other_dc);
            Assert.IsTrue(dist1 < dist2);
        }

        [Test]
        public void TestGetDataCenter()
        {
            RackInferringSnitch rackInferringSnitch = new RackInferringSnitch();

            IPAddress ip = new IPAddress(new byte[] {192, 168, 42, 19});
            string dc = rackInferringSnitch.GetDataCenter(ip);
            Assert.IsTrue(dc == "168");
        }
    }
}