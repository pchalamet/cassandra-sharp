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
    using CassandraSharp;
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Extensibility;
    using NUnit.Framework;

    public abstract class CommonInstanceBuilderTest
    {
        protected abstract IInstanceBuilder GetInstanceBuilder<T>();

        private static IColumnSpec CreateColumnSpec(string name)
        {
            return new ColumnSpec(0, null, null, name, ColumnType.Custom, null, ColumnType.Custom, ColumnType.Custom);
        }

        [Test]
        public void TestBuilding()
        {
            IInstanceBuilder instanceBuilder = GetInstanceBuilder<Toto>();

            instanceBuilder.Set(CreateColumnSpec("NullableInt"), 42);
            instanceBuilder.Set(CreateColumnSpec("NullableIntProperty"), 666);
            instanceBuilder.Set(CreateColumnSpec("Int"), 1);
            instanceBuilder.Set(CreateColumnSpec("IntProperty"), 2);
            instanceBuilder.Set(CreateColumnSpec("String"), "String1");
            instanceBuilder.Set(CreateColumnSpec("StringProperty"), "String2");

            Toto toto = instanceBuilder.Build() as Toto;
            Assert.NotNull(toto);

            Assert.AreEqual(toto.NullableInt, 42);
            Assert.AreEqual(toto.NullableIntProperty, 666);
            Assert.AreEqual(toto.Int, 1);
            Assert.AreEqual(toto.IntProperty, 2);
            Assert.AreEqual(toto.String, "String1");
            Assert.AreEqual(toto.StringProperty, "String2");
        }

        [Test]
        public void TestBuildingCaseInsensitive()
        {
            IInstanceBuilder instanceBuilder = GetInstanceBuilder<Toto>();

            instanceBuilder.Set(CreateColumnSpec("nullableint"), 42);
            instanceBuilder.Set(CreateColumnSpec("nullableintproperty"), 666);
            instanceBuilder.Set(CreateColumnSpec("int"), 1);
            instanceBuilder.Set(CreateColumnSpec("intproperty"), 2);
            instanceBuilder.Set(CreateColumnSpec("string"), "String1");
            instanceBuilder.Set(CreateColumnSpec("stringproperty"), "String2");

            Toto toto = instanceBuilder.Build() as Toto;
            Assert.NotNull(toto);

            Assert.AreEqual(toto.NullableInt, 42);
            Assert.AreEqual(toto.NullableIntProperty, 666);
            Assert.AreEqual(toto.Int, 1);
            Assert.AreEqual(toto.IntProperty, 2);
            Assert.AreEqual(toto.String, "String1");
            Assert.AreEqual(toto.StringProperty, "String2");
        }

        [Test]
        public void TestBuildingUnderscore()
        {
            IInstanceBuilder instanceBuilder = GetInstanceBuilder<Toto>();

            instanceBuilder.Set(CreateColumnSpec("nullable_int"), 42);
            instanceBuilder.Set(CreateColumnSpec("nullable_int_property"), 666);
            instanceBuilder.Set(CreateColumnSpec("int"), 1);
            instanceBuilder.Set(CreateColumnSpec("int_property"), 2);
            instanceBuilder.Set(CreateColumnSpec("string"), "String1");
            instanceBuilder.Set(CreateColumnSpec("string_property"), "String2");

            Toto toto = instanceBuilder.Build() as Toto;
            Assert.NotNull(toto);

            Assert.AreEqual(toto.NullableInt, 42);
            Assert.AreEqual(toto.NullableIntProperty, 666);
            Assert.AreEqual(toto.Int, 1);
            Assert.AreEqual(toto.IntProperty, 2);
            Assert.AreEqual(toto.String, "String1");
            Assert.AreEqual(toto.StringProperty, "String2");
        }

        [Test]
        public void TestNoErrorOnUnknownMember()
        {
            IInstanceBuilder instanceBuilder = GetInstanceBuilder<Toto>();
            instanceBuilder.Set(CreateColumnSpec("boubou"), 1);
        }

        public class Toto
        {
            public int? NullableInt;

            public int? NullableIntProperty { get; set; }

            public int Int;

            public string String;

            public int IntProperty { get; set; }

            public string StringProperty { get; set; }
        }
    }
}