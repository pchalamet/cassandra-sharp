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

    public class DropTableBuilderTestSuite<T> where T : IDialect, new()
    {
        private static IDropTableBuilder CreateDropTableBuilder()
        {
            T dialect = new T();
            IDropTableBuilder builder = dialect.GetDropTableBuilder();
            builder.Table = "TestTable";
            return builder;
        }

        [Test]
        public void TestAllParams()
        {
            const string expectedCql = "drop table TestTable";

            IDropTableBuilder builder = CreateDropTableBuilder();
            string cql = builder.Build();
            Assert.AreEqual(expectedCql, cql);
        }

        [Test]
        public void TestValidateTable()
        {
            IDropTableBuilder builder = CreateDropTableBuilder();
            builder.Table = null;
            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [Test]
        public void TestValidateColumns()
        {
            IDropTableBuilder builder = CreateDropTableBuilder();
            builder.Columns = new string[0];
            Assert.Throws<ArgumentException>(() => builder.Build());
        }
    }
}