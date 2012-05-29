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

    public class CreateTableBuilderTestSuite<T> where T : IDialect, new()
    {
        private static ICreateTableBuilder CreateTableBuilder()
        {
            T dialect = new T();
            ICreateTableBuilder builder = dialect.GetCreateTableBuilder();
            builder.Table = "TestTable";
            builder.Columns = new[] {"A", "B"};
            builder.ColumnTypes = new[] {"text", "int"};
            builder.Keys = new[] {"A"};
            return builder;
        }

        [Test]
        public void TestAllParams()
        {
            const string expectedCql = "create table TestTable (A text,B int,primary key (A))";

            ICreateTableBuilder builder = CreateTableBuilder();
            string cql = builder.Build();
            Assert.AreEqual(expectedCql, cql);
        }

        [Test]
        public void TestCompactStorage()
        {
            const string expectedCql = "create table TestTable (A text,B int,primary key (A)) with compact storage";

            ICreateTableBuilder builder = CreateTableBuilder();
            builder.CompactStorage = true;
            string cql = builder.Build();
            Assert.AreEqual(expectedCql, cql);
        }

        [Test]
        public void TestValidateEmptyColumnTypes()
        {
            ICreateTableBuilder builder = CreateTableBuilder();
            builder.ColumnTypes = new string[0];
            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [Test]
        public void TestValidateEmptyColumns()
        {
            ICreateTableBuilder builder = CreateTableBuilder();
            builder.Columns = new string[0];
            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [Test]
        public void TestValidateMismatchColumnsAndColumnTypes()
        {
            ICreateTableBuilder builder = CreateTableBuilder();
            builder.ColumnTypes = builder.ColumnTypes.Concat(new[] {"int"}).ToArray();
            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [Test]
        public void TestValidateNullColumnTypes()
        {
            ICreateTableBuilder builder = CreateTableBuilder();
            builder.ColumnTypes = null;
            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [Test]
        public void TestValidateNullColumns()
        {
            ICreateTableBuilder builder = CreateTableBuilder();
            builder.Columns = null;
            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [Test]
        public void TestValidateTable()
        {
            ICreateTableBuilder builder = CreateTableBuilder();
            builder.Table = null;
            Assert.Throws<ArgumentException>(() => builder.Build());
        }
    }
}