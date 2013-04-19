//// cassandra-sharp - high performance .NET driver for Apache Cassandra
//// Copyright (c) 2011-2013 Pierre Chalamet
//// 
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
//// 
//// http://www.apache.org/licenses/LICENSE-2.0
//// 
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.

//namespace CassandraSharpUnitTests.Functional
//{
//    using System;
//    using System.Collections.Generic;
//    using System.Linq;
//    using System.Threading.Tasks;
//    using CassandraSharp;
//    using CassandraSharp.CQLPoco;
//    using CassandraSharp.Config;
//    using NUnit.Framework;

//    [TestFixture]
//    public class CollectionTest
//    {
//        public class Cart
//        {
//            public string Id;

//            public ISet<int> Items;
//        }

//        [Test]
//        public void TestSet()
//        {
//            CassandraSharpConfig cassandraSharpConfig = new CassandraSharpConfig();
//            ClusterManager.Configure(cassandraSharpConfig);

//            ClusterConfig clusterConfig = new ClusterConfig
//                {
//                        Endpoints = new EndpointsConfig
//                            {
//                                    Servers = new[] {"localhost"}
//                            }
//                };

//            using (ICluster cluster = ClusterManager.GetCluster(clusterConfig))
//            {
//                ICqlCommand cmd = new PocoCommand(cluster);

//                const string dropFoo = "drop keyspace Tests";

//                try
//                {
//                    cmd.Execute(dropFoo).AsFuture().Wait();
//                }
//                catch
//                {
//                }

//                const string createFoo = "CREATE KEYSPACE Tests WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
//                Console.WriteLine("============================================================");
//                Console.WriteLine(createFoo);
//                Console.WriteLine("============================================================");

//                cmd.Execute(createFoo).AsFuture().Wait();
//                Console.WriteLine();
//                Console.WriteLine();

//                const string createBar = @"create table Tests.cart (id text primary key, items set<int>)";
//                Console.WriteLine("============================================================");
//                Console.WriteLine(createBar);
//                Console.WriteLine("============================================================");
//                cmd.Execute(createBar).AsFuture().Wait();
//                Console.WriteLine();
//                Console.WriteLine();

//                const string cqlInsert = @"insert into Tests.cart (id, items)
//                                             values (?, ?)";
//                var prepInsert = cmd.Prepare(cqlInsert);

//                var iCart = new Cart
//                    {
//                            Id = "1",
//                            Items = new HashSet<int> {1, 2, 3}
//                    };

//                prepInsert.Execute(iCart).AsFuture().Wait();

//                const string cqlSelect = "select * from Tests.cart where id in (?)";
//                IPreparedQuery<Cart> prepSelect = cmd.Prepare<Cart>(cqlSelect);

//                Task<IList<Cart>> sCart = prepSelect.Execute(new {Id = new List<string> {"1", "2"}}).AsFuture();
//                sCart.Wait();

//                var cart = sCart.Result.Single();

//                Assert.AreEqual(cart.Id, "1");
//                Assert.AreEqual(cart.Items.Count, 3);
//                Assert.IsTrue(cart.Items.Contains(1));
//                Assert.IsTrue(cart.Items.Contains(2));
//                Assert.IsTrue(cart.Items.Contains(3));
//            }

//            ClusterManager.Shutdown();
//        }
//    }
//}