// cassandra-sharp - a .NET client for Apache Cassandra
// Copyright (c) 2011-2012 Pierre Chalamet
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

namespace CassandraSharpUnitTests.Pool
{
    using CassandraSharp;
    using CassandraSharp.Pool;
    using NUnit.Framework;

    [TestFixture]
    public class FactoryTest
    {
        private class CustomPool : IPool<IConnection>
        {
            public CustomPool(int poolSize)
            {
                PoolSize = poolSize;
            }

            public int PoolSize { get; private set; }

            public void Dispose()
            {
            }

            public bool Acquire(out IConnection entry)
            {
                entry = null;
                return false;
            }

            public void Release(IConnection entry)
            {
            }
        }

        [Test]
        public void TestCreateCustom()
        {
            const int expectedPoolSize = 42;
            string customType = typeof(CustomPool).AssemblyQualifiedName;
            IPool<IConnection> pool = Factory.Create(customType, expectedPoolSize);

            CustomPool customPool = pool as CustomPool;
            Assert.NotNull(customPool);
            Assert.IsTrue(customPool.PoolSize == expectedPoolSize);
        }

        [Test]
        public void TestCreateStack()
        {
            IPool<IConnection> pool = Factory.Create("Stack", 1);
            Assert.IsTrue(pool is StackPool<IConnection>);
        }

        [Test]
        public void TestCreateVoid()
        {
            IPool<IConnection> pool = Factory.Create("Void", 1);
            Assert.IsTrue(pool is VoidPool<IConnection>);
        }
    }
}