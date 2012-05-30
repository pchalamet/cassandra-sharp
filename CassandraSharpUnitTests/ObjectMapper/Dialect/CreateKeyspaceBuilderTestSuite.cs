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

namespace CassandraSharpUnitTests.ObjectMapper.Dialect
{
    using System;
    using CassandraSharp.ObjectMapper.Dialect;
    using NUnit.Framework;

    public class CreateKeyspaceBuilderTestSuite<T> where T : IDialect, new()
    {
        private static ICreateKeyspaceBuilder CreateCreateKeyspaceBuilder()
        {
            T dialect = new T();
            ICreateKeyspaceBuilder builder = dialect.GetCreateKeyspaceBuilder();
            builder.Keyspace = "TestKeyspace";
            return builder;
        }

        [Test]
        public void TestAllParams()
        {
            const string expectedCql = "create keyspace TestKeyspace with strategy_class=SimpleStrategy and strategy_options:replication_factor=1";

            ICreateKeyspaceBuilder builder = CreateCreateKeyspaceBuilder();
            string cql = builder.Build();
            Assert.AreEqual(expectedCql, cql);
        }

        [Test]
        public void TestValidateKeyspace()
        {
            ICreateKeyspaceBuilder builder = CreateCreateKeyspaceBuilder();
            builder.Keyspace = null;
            Assert.Throws<ArgumentNullException>(() => builder.Build());
        }
    }
}