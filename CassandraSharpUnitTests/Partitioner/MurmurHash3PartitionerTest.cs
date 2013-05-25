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

namespace CassandraSharpUnitTests.Partitioner
{
    using System.Numerics;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Partitioner;
    using NUnit.Framework;

    [TestFixture]
    public class MurmurHash3PartitionerTest
    {
        [Test]
        public void CheckRoutingEmpty()
        {
            IPartitioner partitioner = new Murmur3Partitioner();
            BigInteger? token = partitioner.ComputeToken(new object[0]);
            Assert.IsNull(token);
        }

        [Test]
        public void CheckRoutingNull()
        {
            IPartitioner partitioner = new Murmur3Partitioner();
            BigInteger? token = partitioner.ComputeToken(null);
            Assert.IsNull(token);

            token = partitioner.ComputeToken(new object[0]);
            Assert.IsNull(token);
        }

        [Test]
        public void CheckRoutingSingleKey()
        {
            IPartitioner partitioner = new Murmur3Partitioner();
            BigInteger? token = partitioner.ComputeToken(new object[] {0x12345678});
            Assert.IsTrue(token.HasValue);
            Assert.IsTrue(token.Value == new BigInteger(-8827056344306985898));
        }
    }
}