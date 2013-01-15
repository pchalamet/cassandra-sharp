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

namespace CassandraSharpUnitTests.Functional
{
    using System;
    using System.Linq;
    using System.Net;
    using CassandraSharp;
    using CassandraSharp.CQL;
    using CassandraSharp.CQLPoco;
    using CassandraSharp.Config;
    using CassandraSharpUnitTests.EndpointStrategy;
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

            public string CText;

            public long CTimestamp;

            public Guid CTimeuuid;

            //public string CVarint;

            public Guid CUuid;

            public string CVarchar;
        }

        [Test]
        public void TestAllTypes()
        {
            CassandraSharpConfig cassandraSharpConfig = new CassandraSharpConfig();
            ClusterManager.Configure(cassandraSharpConfig);

            ClusterConfig clusterConfig = new ClusterConfig
                {
                        Endpoints = new EndpointsConfig
                            {
                                    Servers = new[] {"localhost"}
                            }
                };

            using (ICluster cluster = ClusterManager.GetCluster(clusterConfig))
            {
                const string createFoo = "CREATE KEYSPACE Tests WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
                Console.WriteLine("============================================================");
                Console.WriteLine(createFoo);
                Console.WriteLine("============================================================");

                var resCount = cluster.ExecuteNonQuery(createFoo);
                resCount.Wait();
                Console.WriteLine();
                Console.WriteLine();

                // http://www.datastax.com/docs/1.1/references/cql/cql_data_types
                const string createBar = @"CREATE TABLE Tests.AllTypes (cAscii ascii, 
                                                                            cBigint bigint,
                                                                            cBlob blob,
                                                                            cBoolean boolean,
                                                                            cDecimal decimal,
                                                                            cDouble double,
                                                                            cFloat float,
                                                                            cInet inet,
                                                                            cInt int,
                                                                            cText text,
                                                                            cTimestamp timestamp,
                                                                            cTimeuuid timeuuid,
                                                                            cUuid uuid,
                                                                            cVarchar varchar,
                                                                            cVarint varint,
                                                          PRIMARY KEY (cInt))";
                Console.WriteLine("============================================================");
                Console.WriteLine(createBar);
                Console.WriteLine("============================================================");
                resCount = cluster.ExecuteNonQuery(createBar);
                resCount.Wait();
                Console.WriteLine();
                Console.WriteLine();

                const string insertBatch = @"insert into Tests.AllTypes (cAscii, cBigint, cBlob, cBoolean, cDouble, cFloat,
                                                                         cInet, cInt, cText, cTimestamp, cTimeuuid, cUuid, cVarchar)
                                             values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                var prepared = cluster.Prepare(insertBatch);

                var allTypesInsert = new AllTypes
                    {
                            CAscii = "ascii",
                            CBigint = 0x0102030405060708,
                            CBlob = new byte[] {0x1, 0x02, 0x03, 0x04, 0x05},
                            CBoolean = true,
                            CDouble = 1234.5678,
                            CFloat = 234.567f,
                            CInet = new IPAddress(new byte[] {0x01, 0x02, 0x03, 0x04}),
                            CInt = 42,
                            CText = "text",
                            CTimestamp = 0x08070605040301,
                            CTimeuuid = TimedUuid.GenerateTimeBasedGuid(DateTime.Now),
                            CUuid = Guid.NewGuid(),
                            CVarchar = "varchar"
                    };

                prepared.ExecuteNonQuery(allTypesInsert).Wait();

                const string selectAll = "select * from Tests.AllTypes";
                AllTypes allTypesSelect = cluster.Execute<AllTypes>(selectAll).Result.Single();

                const string dropFoo = "drop keyspace Tests";
                resCount = cluster.ExecuteNonQuery(dropFoo);
                resCount.Wait();

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
            }

            ClusterManager.Shutdown();
        }
    }
}