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

using CassandraSharp.Utils;

namespace CassandraSharpUnitTests.Utils
{
    using System;
    using System.Collections.Generic;
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

    public class FactoryWithCustomType : IServiceDescriptor
    {
        private static readonly IDictionary<string, Type> _def = new Dictionary<string, Type>();

        public IDictionary<string, Type> Definition
        {
            get { return _def; }
        }
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

            ITestService testService = ServiceActivator<FactoryWithCustomType>.Create<ITestService>(type, key, value);
            Assert.IsNotNull(testService);
            Assert.IsTrue(key == testService.Key);
            Assert.IsTrue(value == testService.Value);
        }

        [Test]
        public void TestCreateWithEmpty()
        {
            const string type = "";
            const string key = "tralala";
            const int value = 42;

            Assert.Throws<ArgumentException>(() => ServiceActivator<FactoryWithCustomType>.Create<ITestService>(type, key, value));
        }

        [Test]
        public void TestCreateWithNull()
        {
            string type = null;
            const string key = "tralala";
            const int value = 42;

            Assert.Throws<ArgumentException>(() => ServiceActivator<FactoryWithCustomType>.Create<ITestService>(type, key, value));
        }
    }
}