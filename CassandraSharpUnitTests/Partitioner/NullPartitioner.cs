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
    public class NullPartitionerTest
    {
        [Test]
        public void CheckNoToken()
        {
            IPartitioner partitioner = new NullPartitioner();

            BigInteger? token = partitioner.ComputeToken(new object[] {1});
            Assert.IsNull(token);

            token = partitioner.ComputeToken(new object[] {"toto", 42});
            Assert.IsNull(token);
        }
    }
}