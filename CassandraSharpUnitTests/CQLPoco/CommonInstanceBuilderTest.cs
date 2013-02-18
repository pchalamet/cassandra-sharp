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
    using System.Collections.Generic;
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

        private void TestBuildingWithSetter(Dictionary<string, object> data)
        {
            IInstanceBuilder instanceBuilder = GetInstanceBuilder<Toto>();

            foreach (KeyValuePair<string, object> datum in data)
            {
                instanceBuilder.Set(CreateColumnSpec(datum.Key), datum.Value); 
            }

            Toto toto = instanceBuilder.Build() as Toto;
            Assert.NotNull(toto);

            Assert.AreEqual(toto.NullNullableInt, null);
            Assert.AreEqual(toto.NullNullableIntProperty, null);
            Assert.AreEqual(toto.NullableInt, 42);
            Assert.AreEqual(toto.NullableIntProperty, 666);
            Assert.AreEqual(toto.Int, 1);
            Assert.AreEqual(toto.IntProperty, 2);
            Assert.AreEqual(toto.String, "String1");
            Assert.AreEqual(toto.StringProperty, "String2");            
        }


        [Test]
        public void TestBuilding()
        {
            Dictionary<string, object> data = new Dictionary<string, object>
                {
                        {"NullNullableInt", null},
                        {"NullNullableIntProperty", null},
                        {"NullableInt", 42},
                        {"NullableIntProperty", 666},
                        {"Int", 1},
                        {"IntProperty", 2},
                        {"String", "String1"},
                        {"StringProperty", "String2"},
                };

            TestBuildingWithSetter(data);
        }

        [Test]
        public void TestBuildingCaseInsensitive()
        {
            Dictionary<string, object> data = new Dictionary<string, object>
                {
                        {"nullnullableint", null},
                        {"nullnullableintproperty", null},
                        {"nullableint", 42},
                        {"nullableintproperty", 666},
                        {"int", 1},
                        {"intproperty", 2},
                        {"string", "String1"},
                        {"stringproperty", "String2"},
                };

            TestBuildingWithSetter(data);
        }

        [Test]
        public void TestBuildingUnderscore()
        {
            Dictionary<string, object> data = new Dictionary<string, object>
                {
                        {"null_nullable_int", null},
                        {"null_nullable_int_property", null},
                        {"nullable_int", 42},
                        {"nullable_int_property", 666},
                        {"_int_", 1},
                        {"int_property", 2},
                        {"_string_", "String1"},
                        {"string_property", "String2"},
                };

            TestBuildingWithSetter(data);
        }

        [Test]
        public void TestNoErrorOnUnknownMember()
        {
            IInstanceBuilder instanceBuilder = GetInstanceBuilder<Toto>();
            instanceBuilder.Set(CreateColumnSpec("boubou"), 1);
        }

        public class Toto
        {
            public Toto()
            {
                NullNullableInt = 42;
                NullNullableIntProperty = 666;
            }

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