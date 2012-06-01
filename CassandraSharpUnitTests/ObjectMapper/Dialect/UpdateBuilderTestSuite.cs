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
    using System.Linq;
    using CassandraSharp.ObjectMapper.Dialect;
    using NUnit.Framework;

    public class UpdateBuilderTestSuite<T> where T : IDialect, new()
    {
        private static IUpdateBuilder CreateUpdateBuilder()
        {
            T dialect = new T();
            IUpdateBuilder builder = dialect.GetUpdateBuilder();
            builder.Table = "TestTable";
            builder.Columns = new[] { "A", "B" };
            builder.Values = new[] { "1", "2" };
            builder.Wheres = new[] {"C=3"};
            return builder;
        }

        [Test]
        public void TestAllParameters()
        {
            const string expectedCql = "update TestTable set A='1',B='2' where C=3";
            IUpdateBuilder builder = CreateUpdateBuilder();
            string cql = builder.Build();
            Assert.AreEqual(expectedCql, cql);
        }

        [Test]
        public void TestValidateTable()
        {
            IUpdateBuilder builder = CreateUpdateBuilder();
            builder.Table = null;
            Assert.Throws<ArgumentNullException>(() => builder.Build());
        }

        [Test]
        public void TestValidateColumns()
        {
            IUpdateBuilder builder = CreateUpdateBuilder();

            builder.Columns = null;
            Assert.Throws<ArgumentNullException>(() => builder.Build());

            builder.Columns = new string[0];
            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [Test]
        public void TestValidateValues()
        {
            IUpdateBuilder builder = CreateUpdateBuilder();

            builder.Values = null;
            Assert.Throws<ArgumentNullException>(() => builder.Build());

            builder.Values = new string[0];
            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [Test]
        public void TestValidateMismatchColumnsAndValues()
        {
            IUpdateBuilder builder = CreateUpdateBuilder();
            builder.Values = builder.Values.Concat(new[] { "3" }).ToArray();
            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [Test]
        public void TestValidateNullWhere()
        {
            const string expectedCql = "update TestTable set A='1',B='2'";
            IUpdateBuilder builder = CreateUpdateBuilder();
            builder.Wheres = null;
            string cql = builder.Build();
            Assert.AreEqual(expectedCql, cql);
        }
    }
}