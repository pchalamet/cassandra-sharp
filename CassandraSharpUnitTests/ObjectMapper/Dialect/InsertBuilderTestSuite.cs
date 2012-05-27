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

    public class InsertBuilderTestSuite<T> where T : IDialect, new()
    {
        private static IInsertBuilder CreateInsertBuilder()
        {
            T dialect = new T();
            IInsertBuilder builder = dialect.GetInsertBuilder();
            builder.Table = "TestTable";
            builder.Columns = new[] {"A", "B"};
            builder.Values = new[] {"1", "2"};
            return builder;
        }

        [Test]
        public void TestAllParameters()
        {
            const string expectedCql = "insert into TestTable (A,B) values (1,2)";
            IInsertBuilder builder = CreateInsertBuilder();
            string cql = builder.Build();
            Assert.AreEqual(expectedCql, cql);
        }

        [Test]
        public void TestValidateTable()
        {
            IInsertBuilder builder = CreateInsertBuilder();
            builder.Table = null;
            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [Test]
        public void TestValidateNullColumns()
        {
            IInsertBuilder builder = CreateInsertBuilder();
            builder.Columns = null;
            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [Test]
        public void TestValidateEmptyColumns()
        {
            IInsertBuilder builder = CreateInsertBuilder();
            builder.Columns = new string[0];
            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [Test]
        public void TestValidateNullValues()
        {
            IInsertBuilder builder = CreateInsertBuilder();
            builder.Values = null;
            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [Test]
        public void TestValidateEmptyValues()
        {
            IInsertBuilder builder = CreateInsertBuilder();
            builder.Values = new string[0];
            Assert.Throws<ArgumentException>(() => builder.Build());
        }

        [Test]
        public void TestValidateMismatchColumnsAndValues()
        {
            IInsertBuilder builder = CreateInsertBuilder();
            builder.Values = builder.Values.Union(new[] {"3"}).ToArray();
            Assert.Throws<ArgumentException>(() => builder.Build());
        }
    }
}