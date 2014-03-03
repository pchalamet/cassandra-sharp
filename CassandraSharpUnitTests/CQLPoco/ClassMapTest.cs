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

namespace CassandraSharpUnitTests.CQLPoco
{
    using NUnit.Framework;
    using System;
    using System.Linq;
    using CassandraSharp.CQLPoco;

    [TestFixture]
    public class ClassMapTest
    {
        private ClassMap<TestPoco> classMap;

        private TestPoco pocoInstance;

        [SetUp]
        public void SetUp()
        {
            classMap = new ClassMap<TestPoco>();
            pocoInstance = new TestPoco
                               {
                                   ComplexType = new PocoPoint { X = 1, Y = 2 },
                                   DifferentName = "Another",
                                   IgnoredProperty = 123,
                                   TestProperty = "Property",
                                   TestField = "Field"
                               };
        }

        [Test]
        public void Field_ShouldBeAccessible()
        {
            var member = classMap.GetMember("TestField");
            Assert.IsNotNull(member);

            Assert.AreEqual("Field", member.GetValue(pocoInstance));
            member.SetValue(pocoInstance, "new");
            Assert.AreEqual("new", member.GetValue(pocoInstance));
        }

        [Test]
        public void Property_ShouldBeAccessible()
        {
            var member = classMap.GetMember("TestProperty");
            Assert.IsNotNull(member);

            Assert.AreEqual("Property", member.GetValue(pocoInstance));
            member.SetValue(pocoInstance, "new");
            Assert.AreEqual("new", member.GetValue(pocoInstance));
        }

        [Test]
        public void CustomColumnName_ShouldBeAccessible()
        {
            var member = classMap.GetMember("AnotherColumn");
            Assert.IsNotNull(member);

            Assert.AreEqual("Another", member.GetValue(pocoInstance));
            member.SetValue(pocoInstance, "new");
            Assert.AreEqual("new", member.GetValue(pocoInstance));
        }

        [Test]
        public void ComplexType_ShouldBeAccessible()
        {
            var member = classMap.GetMember("ComplexType");
            Assert.IsNotNull(member);

            var point = member.GetValue(pocoInstance) as PocoPoint;
            Assert.IsNotNull(point);
            Assert.AreEqual(1, point.X);
            Assert.AreEqual(2, point.Y);

            member.SetValue(pocoInstance, new PocoPoint { X = 100, Y = 200 });

            var newPoint = member.GetValue(pocoInstance) as PocoPoint;
            Assert.IsNotNull(newPoint);
            Assert.AreEqual(100, newPoint.X);
            Assert.AreEqual(200, newPoint.Y);
        }

        [Test]
        public void ComplexType_ShouldHaveCustomSerializer()
        {
            var member = classMap.GetMember("ComplexType");
            Assert.IsNotNull(member.ValueSerializer);

            var rawData = member.ValueSerializer.Serialize(pocoInstance.ComplexType);
            Assert.IsNotNull(rawData);

            var deserializedPoint = (PocoPoint)member.ValueSerializer.Deserialize(rawData);
            Assert.AreEqual(pocoInstance.ComplexType.X, deserializedPoint.X);
            Assert.AreEqual(pocoInstance.ComplexType.Y, deserializedPoint.Y);
        }

        [Test]
        public void IgnoredProperty_ShouldBeIgnored()
        {
            var member = classMap.GetMember("IgnoredProperty");
            Assert.IsNull(member);
        }

        #region Test types

        private class TestPoco
        {
            public string TestField;

            public string TestProperty { get; set; }

            [CqlColumn("AnotherColumn")]
            public string DifferentName { get; set; }

            public PocoPoint ComplexType { get; set; }

            [CqlIgnore]
            public int IgnoredProperty { get; set; }
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
