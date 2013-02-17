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

namespace CassandraSharpUnitTests.CQLPoco
{
    using System;
    using CassandraSharp;
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Extensibility;
    using NUnit.Framework;

    public abstract class CommonDataSourceTest
    {
        protected abstract IDataSource GetDataSource<T>();

        private static IColumnSpec CreateColumnSpec(string name)
        {
            return new ColumnSpec(0, null, null, name, ColumnType.Custom, null, ColumnType.Custom, ColumnType.Custom);
        }

        [Test]
        public void TestReading()
        {
            IDataSource dataSource = GetDataSource<Toto>();

            Assert.AreEqual(dataSource.Get(CreateColumnSpec("NullNullableInt")), null);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("NullNullableIntProperty")), null);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("NullableInt")), 42);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("NullableIntProperty")), 666);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("Int")), 1);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("IntProperty")), 2);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("String")), "String1");
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("StringProperty")), "String2");
        }

        [Test]
        public void TestReadingCaseInsensitive()
        {
            IDataSource dataSource = GetDataSource<Toto>();

            Assert.AreEqual(dataSource.Get(CreateColumnSpec("nullnullableint")), null);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("nullnullableintproperty")), null);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("nullableint")), 42);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("nullableintproperty")), 666);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("int")), 1);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("intproperty")), 2);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("string")), "String1");
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("stringproperty")), "String2");
        }

        [Test]
        public void TestReadingUnderscore()
        {
            IDataSource dataSource = GetDataSource<Toto>();

            Assert.AreEqual(dataSource.Get(CreateColumnSpec("Null_nullable_Int")), null);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("null_nullable_int_Property")), null);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("Nullable_int")), 42);
            Assert.AreEqual(dataSource.Get(CreateColumnSpec("Nullable_int_Property")), 666);
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
            public int? NullableInt;

            public int? NullableIntProperty { get; set; }

            public int? NullNullableInt;

            public int? NullNullableIntProperty { get; set; }

            public int Int;

            public string String;

            public int IntProperty { get; set; }

            public string StringProperty { get; set; }
        }
    }
}