// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2018 Pierre Chalamet
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

using CassandraSharp.Utils;

namespace CassandraSharpUnitTests.EndpointStrategy
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using CassandraSharp.Extensibility;
    using NUnit.Framework;

    [TestFixture]
    public class RandomEndpointStrategyTest
    {
        [Test]
        public void TestRandomness()
        {
            IPAddress[] ips = new IPAddress[4];
            for (byte i = 0; i < ips.Length; ++i)
            {
                ips[i] = new IPAddress(new byte[] {192, 168, 0, i});
            }

            IEndpointStrategy endpointStrategy = ServiceActivator<CassandraSharp.EndpointStrategy.Factory>.Create<IEndpointStrategy>("Random", ips.AsEnumerable());

            List<IPAddress> alls = new List<IPAddress>();
            for (int i = 0; i < 10000; ++i)
            {
                IPAddress nextEndpoint = endpointStrategy.Pick();
                if (! alls.Contains(nextEndpoint))
                {
                    alls.Add(nextEndpoint);
                }
            }

            foreach (IPAddress ip in alls)
            {
                Assert.IsTrue(alls.Contains(ip));
            }
        }
    }
}