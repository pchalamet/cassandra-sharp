// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2018 Pierre Chalamet
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

using CassandraSharp.CQLBinaryProtocol;

namespace CassandraSharpUnitTests.CQLBinaryProtocol
{
    using CassandraSharp.CQLPoco;
    using NUnit.Framework;
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Linq;

    [TestFixture]
    public class ValueSerializerTest
    {
        [Test]
        public void SerializeInt()
        {
            var serializer = new ValueSerializer<int>();
            var data = serializer.Serialize(123);
            Assert.IsNotNull(data);
            Assert.Greater(data.Length, 0);
            Assert.AreEqual(123, serializer.Deserialize(data));
        }

        [Test]
        public void SerializeEnum()
        {
            var serializer = new ValueSerializer<UriHostNameType>();
            var data = serializer.Serialize(UriHostNameType.IPv4);
            Assert.IsNotNull(data);
            Assert.Greater(data.Length, 0);
            Assert.AreEqual(UriHostNameType.IPv4, (UriHostNameType)serializer.Deserialize(data));
        }

        [Test]
        public void SerializeListOfString()
        {
            var serializer = new ValueSerializer<List<string>>();
            var data = serializer.Serialize(new List<string> { "a", "b", "cde" });
            Assert.IsNotNull(data);
            Assert.Greater(data.Length, 0);
            var list = (List<string>)serializer.Deserialize(data);

            Assert.AreEqual(3, list.Count);
            Assert.AreEqual("a", list[0]);
            Assert.AreEqual("cde", list[2]);
        }

        [Test]
        public void SerializeDictionaryOfLongAndBlobs()
        {
            var serializer = new ValueSerializer<Dictionary<long, byte[]>>();
            var data = serializer.Serialize(new Dictionary<long, byte[]> { { 1, new byte[] { 1, 2, 3 } }, { 2, new byte[] { } } });
            Assert.IsNotNull(data);
            Assert.Greater(data.Length, 0);
            var dict = (Dictionary<long, byte[]>)serializer.Deserialize(data);

            Assert.AreEqual(2, dict.Count);
            Assert.AreEqual(3, dict[1].Length);
            Assert.AreEqual(2, dict[1][1]);
            Assert.AreEqual(0, dict[2].Length);
        }

        [Test]
        public void SerializeHashSet()
        {
            var serializer = new ValueSerializer<HashSet<int>>();
            var data = serializer.Serialize(new HashSet<int> { 1, 2 });
            Assert.IsNotNull(data);
            Assert.Greater(data.Length, 0);
            var set = (HashSet<int>)serializer.Deserialize(data);

            Assert.AreEqual(2, set.Count);
            Assert.IsTrue(set.Contains(1));
            Assert.IsTrue(set.Contains(2));
            Assert.IsFalse(set.Contains(3));
        }

        [Test]
        public void SerializeCustomCollection()
        {
            var serializer = new ValueSerializer<CustomCollection>();
            var data = serializer.Serialize(new CustomCollection { 1, 2, 3 });
            Assert.IsNotNull(data);
            Assert.Greater(data.Length, 0);
            var list = (CustomCollection)serializer.Deserialize(data);

            Assert.AreEqual(3, list.Count);
            Assert.AreEqual(1, list[0]);
        }

        [Test]
        public void SerializeCustomType()
        {
            var serializer = new ValueSerializer<Point>();
            var data = serializer.Serialize(new Point { X = 1, Y = 2 });
            Assert.IsNotNull(data);
            Assert.Greater(data.Length, 0);
            var point = (Point)serializer.Deserialize(data);

            Assert.AreEqual(1, point.X);
            Assert.AreEqual(2, point.Y);
        }

        [Test]
        public void SerializeListOfCustomType()
        {
            var serializer = new ValueSerializer<List<Point>>();
            var data = serializer.Serialize(new List<Point> { new Point { X = 1, Y = 2 } });
            Assert.IsNotNull(data);
            Assert.Greater(data.Length, 0);
            var pointList = (List<Point>)serializer.Deserialize(data);
            Assert.AreEqual(1, pointList.Count);
            var point = pointList[0];

            Assert.AreEqual(1, point.X);
            Assert.AreEqual(2, point.Y);
        }

        [Test]
        public void SerializeMapOfCustomType()
        {
            var serializer = new ValueSerializer<Dictionary<string, Point>>();
            var data = serializer.Serialize(new Dictionary<string, Point> { { "TopLeft", new Point { X = 1, Y = 2 } }, { "BottomRight", new Point { X = 5, Y = 7 } } });
            Assert.IsNotNull(data);
            Assert.Greater(data.Length, 0);
            var dict = (Dictionary<string, Point>)serializer.Deserialize(data);
            Assert.AreEqual(2, dict.Count);
            var point = dict["BottomRight"];

            Assert.AreEqual(5, point.X);
            Assert.AreEqual(7, point.Y);
        }

        #region Custom types
        public class CustomCollection : Collection<int>
        {
        }

        [CassandraTypeSerializer(typeof(PointSerializer))]
        public class Point
        {
            public int X { get; set; }

            public int Y { get; set; }
        }

        public class PointSerializer : ICassandraTypeSerializer
        {
            public byte[] Serialize(object value)
            {
                var val = value as Point;
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

                var val = new Point();
                val.X = BitConverter.ToInt32(data, 0);
                val.Y = BitConverter.ToInt32(data, 4);
                return val;
            }
        }

        #endregion
    }
}
