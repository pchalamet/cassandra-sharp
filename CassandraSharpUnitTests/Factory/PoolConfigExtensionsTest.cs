// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
// limitations under the License.
namespace CassandraSharpUnitTests.Factory
{
    using CassandraSharp;
    using CassandraSharp.Config;
    using CassandraSharp.Factory;
    using CassandraSharp.Pool;
    using NUnit.Framework;

    [TestFixture]
    public class PoolConfigExtensionsTest
    {
        [Test]
        public void TestCreateStack()
        {
            PoolType poolType = PoolType.Stack;
            IPool<IConnection> pool = poolType.Create(1);
            Assert.IsTrue(pool is StackPool<IConnection>);
        }

        [Test]
        public void TestCreateVoid()
        {
            PoolType poolType = PoolType.Void;
            IPool<IConnection> pool = poolType.Create(1);
            Assert.IsTrue(pool is VoidPool<IConnection>);
        }
    }
}