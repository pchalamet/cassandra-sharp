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

namespace CassandraSharpUnitTests.Utils
{
    using CassandraSharp.Utils;
    using NUnit.Framework;

    public interface ITestService
    {
        string Key { get; }

        int Value { get; }
    }

    public class TestService : ITestService
    {
        public TestService(string key, int value)
        {
            Key = key;
            Value = value;
        }

        public string Key { get; private set; }

        public int Value { get; private set; }
    }

    [TestFixture]
    public class ServiceActivatorTest
    {
        [Test]
        public void TestCreateWithCustomType()
        {
            string type = typeof(TestService).AssemblyQualifiedName;
            const string key = "tralala";
            const int value = 42;

            ITestService testService = ServiceActivator.Create<ITestService>(type, key, value);
            Assert.IsNotNull(testService);
            Assert.IsTrue(key == testService.Key);
            Assert.IsTrue(value == testService.Value);
        }

        [Test]
        public void TestCreateWithNull()
        {
            string type = null;
            const string key = "tralala";
            const int value = 42;

            ITestService testService = ServiceActivator.Create<ITestService>(type, key, value);
            Assert.IsNull(testService);
        }

        [Test]
        public void TestCreateWithEmpty()
        {
            string type = "";
            const string key = "tralala";
            const int value = 42;

            ITestService testService = ServiceActivator.Create<ITestService>(type, key, value);
            Assert.IsNull(testService);
        }
    }
}