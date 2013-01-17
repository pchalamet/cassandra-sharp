// cassandra-sharp - a .NET client for Apache Cassandra
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

namespace CassandraSharpUnitTests.CQLPoco
{
    using System;
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Extensibility;
    using NUnit.Framework;

    public abstract class CommonDataSourceTest
    {
        protected abstract IDataSource GetDataSource<T>() where T : new();

        private static IColumnSpec CreateColumnSpec(string name)
        {
            return new ColumnSpec(0, null, null, name, ColumnType.Custom, null, ColumnType.Custom, ColumnType.Custom);
        }

        [Test]
        public void TestReading()
        {
            IDataSource dataSource = GetDataSource<Toto>();

            Assert.AreEqual(dataSource.Get(CreateColumnSpec("Int")), 1);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("IntProperty")), 2);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("String")), "String1");
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("StringProperty")), "String2");
        }

        [Test]
        public void TestReadingCaseInsensitive()
        {
            IDataSource dataSource = GetDataSource<Toto>();

            Assert.AreEqual(dataSource.Get(CreateColumnSpec("int")), 1);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("intproperty")), 2);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("string")), "String1");
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("stringproperty")), "String2");
        }

        [Test]
        public void TestReadingUnderscore()
        {
            IDataSource dataSource = GetDataSource<Toto>();

            Assert.AreEqual(dataSource.Get(CreateColumnSpec("Int")), 1);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("Int_Property")), 2);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("String")), "String1");
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("String_Property")), "String2");
        }

        [Test]
        public void TestErrorOnUnknownMember()
        {
            IDataSource dataSource = GetDataSource<Toto>();
            Assert.Throws(typeof(ArgumentException), () => dataSource.Get(CreateColumnSpec("boubou")));
        }

        public class Toto
        {
            public int Int;

            public string String;

            public int IntProperty { get; set; }

            public string StringProperty { get; set; }
        }
    }
}