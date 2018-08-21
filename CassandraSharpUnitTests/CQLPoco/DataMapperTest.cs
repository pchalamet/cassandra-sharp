// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2014 Pierre Chalamet
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

using CassandraSharp.Core.CQLPoco;
using CassandraSharp.CQLBinaryProtocol;

namespace CassandraSharpUnitTests.CQLPoco
{
    using CassandraSharp;
    using CassandraSharp.Exceptions;
    using CassandraSharp.CQLPoco;
    using NUnit.Framework;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using CassandraSharp.Extensibility;

    [TestFixture]
    public class DataMapperTest
    {
        private readonly ValueSerializer<string> stringSerializer = new ValueSerializer<string>();

        private readonly ValueSerializer<PocoPoint> pointSerializer = new ValueSerializer<PocoPoint>();

        private DataMapper<TestPoco> dataMapper;

        private TestPoco pocoInstance;

        [SetUp]
        public void SetUp()
        {
            dataMapper = new DataMapper<TestPoco>();
            pocoInstance = new TestPoco
            {
                ComplexType = new PocoPoint { X = 1, Y = 2 },
                DifferentName = "Another",
                TestProperty = "Property",
                TestField = "Field"
            };
        }

        [Test]
        public void MapToColumns_Existing_MappedAndSerialized()
        {
            var columnSpecs = new List<IColumnSpec>
                                  {
                                      CreateColumnSpec("TestField"),
                                      CreateColumnSpec("ComplexType"),
                                      CreateColumnSpec("AnotherColumn")
                                  };

            var columnData = dataMapper.MapToColumns(pocoInstance, columnSpecs).ToList();

            // Colums should come in same order, as requested
            Assert.AreEqual(columnSpecs[0], columnData[0].ColumnSpec);
            Assert.AreEqual(columnSpecs[1], columnData[1].ColumnSpec);
            Assert.AreEqual(columnSpecs[2], columnData[2].ColumnSpec);

            // Data should be mapped and serialized using correct serializer
            Assert.AreEqual(pocoInstance.TestField, stringSerializer.Deserialize(columnData[0].RawData));
            var pocoPoint = (PocoPoint)pointSerializer.Deserialize(columnData[1].RawData);

            Assert.AreEqual(pocoInstance.ComplexType.X, pocoPoint.X);
            Assert.AreEqual(pocoInstance.ComplexType.Y, pocoPoint.Y);
            Assert.AreEqual(pocoInstance.DifferentName, stringSerializer.Deserialize(columnData[2].RawData));
        }

        [Test]
        public void MapToColumns_ShouldBeUnderscoreAndCaseInsensitive()
        {
            var columnSpecs = new List<IColumnSpec>
                                  {
                                      CreateColumnSpec("Test_Field"),
                                      CreateColumnSpec("complex_Type"),
                                      CreateColumnSpec("another_Column")
                                  };

            var columnData = dataMapper.MapToColumns(pocoInstance, columnSpecs).ToList();

            // Colums should come in same order, as requested
            Assert.AreEqual(columnSpecs[0], columnData[0].ColumnSpec);
            Assert.AreEqual(columnSpecs[1], columnData[1].ColumnSpec);
            Assert.AreEqual(columnSpecs[2], columnData[2].ColumnSpec);

            // Data should be mapped and serialized using correct serializer
            Assert.AreEqual(pocoInstance.TestField, stringSerializer.Deserialize(columnData[0].RawData));
            var pocoPoint = (PocoPoint)pointSerializer.Deserialize(columnData[1].RawData);

            Assert.AreEqual(pocoInstance.ComplexType.X, pocoPoint.X);
            Assert.AreEqual(pocoInstance.ComplexType.Y, pocoPoint.Y);
            Assert.AreEqual(pocoInstance.DifferentName, stringSerializer.Deserialize(columnData[2].RawData));
        }

        [Test]
        public void MapToObject_Existing_DeserializedAndMapped()
        {
            var pocoPoint = new PocoPoint { X = 100, Y = 200 };
            var columnData = new List<IColumnData>
                                  {
                                      new ColumnData(CreateColumnSpec("TestField"), stringSerializer.Serialize("abcde")),
                                      new ColumnData(CreateColumnSpec("ComplexType"), pointSerializer.Serialize(pocoPoint)),
                                      new ColumnData(CreateColumnSpec("AnotherColumn"), stringSerializer.Serialize("qwerty"))
                                  };

            var poco = (TestPoco)dataMapper.MapToObject(columnData);

            Assert.AreEqual("abcde", poco.TestField);
            Assert.AreEqual("qwerty", poco.DifferentName);
            Assert.AreEqual(100, poco.ComplexType.X);
            Assert.AreEqual(200, poco.ComplexType.Y);
        }

        [Test]
        public void MapToObject__ShouldBeUnderscoreAndCaseInsensitive()
        {
            var pocoPoint = new PocoPoint { X = 100, Y = 200 };
            var columnData = new List<IColumnData>
                                  {
                                      new ColumnData(CreateColumnSpec("test_Field"), stringSerializer.Serialize("abcde")),
                                      new ColumnData(CreateColumnSpec("complex_Type"), pointSerializer.Serialize(pocoPoint)),
                                      new ColumnData(CreateColumnSpec("Another_Column"), stringSerializer.Serialize("qwerty"))
                                  };

            var poco = (TestPoco)dataMapper.MapToObject(columnData);

            Assert.AreEqual("abcde", poco.TestField);
            Assert.AreEqual("qwerty", poco.DifferentName);
            Assert.AreEqual(100, poco.ComplexType.X);
            Assert.AreEqual(200, poco.ComplexType.Y);
        }

        [Test]
        public void MapToColumns_NotExisting_ThrowsException()
        {
            var columnSpecs = new List<IColumnSpec>
                                  {
                                      CreateColumnSpec("WontFindMe"),
                                      CreateColumnSpec("ComplexType"),
                                      CreateColumnSpec("AnotherColumn")
                                  };

            Assert.Throws<DataMappingException>(() => dataMapper.MapToColumns(pocoInstance, columnSpecs).ToList());
        }

        [Test]
        public void MapToObject_NotExisting_SilentlyIgnores()
        {
            var columnData = new List<IColumnData>
                                  {
                                      new ColumnData(CreateColumnSpec("WontFindMe"), stringSerializer.Serialize("abcde")),     
                                      new ColumnData(CreateColumnSpec("AnotherColumn"), stringSerializer.Serialize("qwerty"))
                                  };

            var poco = (TestPoco)dataMapper.MapToObject(columnData);
            Assert.IsNotNull(poco);
        }

        private IColumnSpec CreateColumnSpec(string columnName)
        {
            return new ColumnSpec(0, null, null, columnName, ColumnType.Custom, null, ColumnType.Custom, ColumnType.Custom);
        }

        #region Test types

        private class TestPoco
        {
            public string TestField;

            public string TestProperty { get; set; }

            [CqlColumn("AnotherColumn")]
            public string DifferentName { get; set; }

            public PocoPoint ComplexType { get; set; }
        }

        [CassandraTypeSerializer(typeof(PocoPointSerializer))]
        private class PocoPoint
        {
            public int X { get; set; }

            public int Y { get; set; }
        }

        private class PocoPointSerializer : ICassandraTypeSerializer
        {
            public byte[] Serialize(object value)
            {
                var val = value as PocoPoint;
                if (val == null)
                {
                    return null;
                }

                return BitConverter.GetBytes(val.X).Concat(BitConverter.GetBytes(val.Y)).ToArray();
            }

            public object Deserialize(byte[] data)
            {
                if (data == null || data.Length != 8)
                {
                    return null;
                }

                return new PocoPoint
                {
                    X = BitConverter.ToInt32(data, 0),
                    Y = BitConverter.ToInt32(data, 4)
                };
            }
        }

        #endregion
    }
}
