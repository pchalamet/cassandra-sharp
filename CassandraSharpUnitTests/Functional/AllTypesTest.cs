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

namespace CassandraSharpUnitTests.Functional
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using CassandraSharp;
    using CassandraSharp.CQLPoco;
    using CassandraSharp.Config;
    using CassandraSharp.Utils;
    using NUnit.Framework;

    [TestFixture]
    public class AllTypesTest
    {
        public class AllTypes
        {
            public string CAscii;

            public long CBigint;

            public byte[] CBlob;

            public bool CBoolean;

            //            public long CCounter;

            //            public long CDecimal;

            public double CDouble;

            public float CFloat;

            public IPAddress CInet;

            public int CInt;

            public List<int> CList;

            public Dictionary<string, int> CMap;

            public HashSet<int> CSet;

            public string CText;

            public DateTime CTimestamp;

            public Guid CTimeuuid;

            //public string CVarint;

            public TestEnum CEnum;

            public Guid CUuid;

            public string CVarchar;

            public Point CPoint;
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

        public enum TestEnum
        {
            ValueA,
            ValueB
        }

        [Test]
        public void TestAllTypes()
        {
            CassandraSharpConfig cassandraSharpConfig = new CassandraSharpConfig();
            using (var clusterManager = new ClusterManager(cassandraSharpConfig))
            {

                ClusterConfig clusterConfig = new ClusterConfig
                {
                    Endpoints = new EndpointsConfig
                    {
                        Servers = new[] { "cassandra1" }
                    }
                };

                using (ICluster cluster = clusterManager.GetCluster(clusterConfig))
                {
                    ICqlCommand cmd = cluster.CreatePocoCommand();

                    const string dropFoo = "drop keyspace Tests";

                    try
                    {
                        cmd.Execute(dropFoo).AsFuture().Wait();
                    }
                    catch
                    {
                    }

                    const string createFoo = "CREATE KEYSPACE Tests WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
                    Console.WriteLine("============================================================");
                    Console.WriteLine(createFoo);
                    Console.WriteLine("============================================================");

                    cmd.Execute(createFoo).AsFuture().Wait();
                    Console.WriteLine();
                    Console.WriteLine();

                    // http://www.datastax.com/docs/1.1/references/cql/cql_data_types
                    const string createBar = @"CREATE TABLE Tests.AllTypes (cAscii ascii, 
                                                                            cBigint bigint,
                                                                            cBlob blob,
                                                                            cBoolean boolean,
                                                                            
                                                                            cDouble double,
                                                                            cFloat float,
                                                                            cInet inet,
                                                                            cInt int,
                                                                            cText text,
                                                                            cTimestamp timestamp,
                                                                            cTimeuuid timeuuid,
                                                                            cUuid uuid,
                                                                            cVarchar varchar,
                                                                           
                                                                            cList list<int>,
                                                                            cSet set<int>,
                                                                            cMap map<text, int>,
                                                                            cEnum int,
                                                                            
                                                                            cPoint blob,
                                                          PRIMARY KEY (cInt))";
                    Console.WriteLine("============================================================");
                    Console.WriteLine(createBar);
                    Console.WriteLine("============================================================");
                    cmd.Execute(createBar).AsFuture().Wait();
                    Console.WriteLine();
                    Console.WriteLine();

                    const string insertBatch = @"insert into Tests.AllTypes (cAscii, cBigint, cBlob, cBoolean, cDouble, cFloat,
                                                                         cInet, cInt, cText, cTimestamp, cTimeuuid, cUuid, cVarchar, cList, cSet, cMap, cEnum, cPoint)
                                             values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    var prepared = cmd.Prepare(insertBatch);

                    var allTypesInsert = new AllTypes
                    {
                        CAscii = new string('x', 8000),
                        CBigint = 0x0102030405060708,
                        CBlob = Enumerable.Repeat((byte)42, 33000).ToArray(),
                        CBoolean = true,
                        CDouble = 1234.5678,
                        CFloat = 234.567f,
                        CInet = new IPAddress(new byte[] { 0x01, 0x02, 0x03, 0x04 }),
                        CInt = 42,
                        CText = new string('x', 100000),
                        CTimestamp = new DateTime(2013, 1, 16, 14, 20, 0),
                        CTimeuuid = TimedUuid.GenerateTimeBasedGuid(DateTime.Now),
                        CUuid = Guid.NewGuid(),
                        CVarchar = new string('x', 5000),
                        CList = new List<int> { 1, 2, 3 },
                        CSet = new HashSet<int> { 1, 2, 3 },
                        CMap = new Dictionary<string, int> { { "one", 1 }, { "two", 2 }, { new string('x', 65525), 3 } },
                        CEnum = TestEnum.ValueB,
                        CPoint = new Point { X = 1, Y = 3 }
                    };

                    prepared.Execute(allTypesInsert).AsFuture().Wait();

                    const string selectAll = "select * from Tests.AllTypes";
                    AllTypes allTypesSelect = cmd.Execute<AllTypes>(selectAll).AsFuture().Result.Single();

                    cmd.Execute(dropFoo).AsFuture().Wait();

                    Assert.AreEqual(allTypesInsert.CAscii, allTypesSelect.CAscii);
                    Assert.AreEqual(allTypesInsert.CBigint, allTypesSelect.CBigint);
                    Assert.AreEqual(allTypesInsert.CBlob, allTypesSelect.CBlob);
                    Assert.AreEqual(allTypesInsert.CBoolean, allTypesSelect.CBoolean);
                    Assert.AreEqual(allTypesInsert.CDouble, allTypesSelect.CDouble);
                    Assert.AreEqual(allTypesInsert.CFloat, allTypesSelect.CFloat);
                    Assert.AreEqual(allTypesInsert.CInet, allTypesSelect.CInet);
                    Assert.AreEqual(allTypesInsert.CInt, allTypesSelect.CInt);
                    Assert.AreEqual(allTypesInsert.CText, allTypesSelect.CText);
                    Assert.AreEqual(allTypesInsert.CTimestamp, allTypesSelect.CTimestamp);
                    Assert.AreEqual(allTypesInsert.CTimeuuid, allTypesSelect.CTimeuuid);
                    Assert.AreEqual(allTypesInsert.CUuid, allTypesSelect.CUuid);
                    Assert.AreEqual(allTypesInsert.CVarchar, allTypesSelect.CVarchar);
                    Assert.AreEqual(allTypesInsert.CList, allTypesSelect.CList);
                    Assert.AreEqual(allTypesInsert.CSet, allTypesSelect.CSet);
                    Assert.AreEqual(allTypesInsert.CMap, allTypesSelect.CMap);
                    Assert.AreEqual(allTypesInsert.CEnum, allTypesSelect.CEnum);

                    Assert.AreEqual(allTypesInsert.CPoint.X, allTypesSelect.CPoint.X);
                    Assert.AreEqual(allTypesInsert.CPoint.Y, allTypesSelect.CPoint.Y);
                }
            }
        }
    }
}