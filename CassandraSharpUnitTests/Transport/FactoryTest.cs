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

namespace CassandraSharpUnitTests.Transport
{
    using CassandraSharp;
    using CassandraSharp.Config;
    using CassandraSharp.Transport;
    using NUnit.Framework;

    [TestFixture]
    public class FactoryTest
    {
        [Test]
        public void TestCreateBuffered()
        {
            TransportConfig config = new TransportConfig {Type = "Buffered"};

            ITransportFactory transport = Factory.Create(config);
            Assert.IsTrue(transport is BufferedTransportFactory);
        }

        [Test]
        public void TestCreateFramed()
        {
            TransportConfig config = new TransportConfig {Type = "Framed"};

            ITransportFactory transport = Factory.Create(config);
            Assert.IsTrue(transport is FramedTransportFactory);
        }
    }
}