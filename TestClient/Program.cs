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

namespace TestClient
{
    using System;
    using System.Collections.Generic;
    using Apache.Cassandra;
    using CassandraSharp;
    using CassandraSharp.Config;
    using CassandraSharp.MadeSimple;
    using CassandraSharp.NameOrValues;
    using CassandraSharp.ObjectMapper;

    [Schema("TestKeyspace", Comment = "People table")]
    public class PeopleSchema
    {
        [Index(Name = "birthyear")]
        public int Birthyear;

        [Key(Name = "firstname")]
        public string FirstName;

        [Column(Name = "lastname")]
        public string LastName;
    }

    internal class Program
    {
        private static void Main(string[] args)
        {
            XmlConfigurator.Configure();

            // using declarative configuration
            using (ICluster cluster = ClusterManager.GetCluster("TestCassandra"))
            {
                try
                {
                    TestCluster(cluster);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
            }

            //// using minimal declarative configuration
            //// no keyspace is specified : we need to set ICommandInfo before calling the command
            //using (ICluster cluster = ClusterManager.GetCluster("TestCassandraMinimal"))
            //{
            //    BehaviorConfigBuilder cmdInfoBuilder = new BehaviorConfigBuilder { KeySpace = "TestKS" };
            //    ICluster configuredCluster = cluster.Configure(cmdInfoBuilder);
            //    TestCluster(configuredCluster);
            //}

            ClusterManager.Shutdown();
        }

        private static void TestCluster(ICluster cluster)
        {
            //try
            //{
            //    cluster.Drop<PeopleSchema>();
            //}
            //catch
            //{
            //}
            //cluster.Create<PeopleSchema>();

            //cluster.Execute("update PeopleSchema set lastname=?, birthyear=?, toto=? where firstname=?",
            //                new { firstname = "isabelle", lastname = "chalamet", birthyear = 1972 },
            //                new { firstname = "pierre", lastname = "chalamet", birthyear = 1973 });

            cluster.Execute("insert into PeopleSchema (firstname, lastname, birthyear) values (?, ?, ?)",
                            new {firstname = "isabelle", lastname = "chalamet", birthyear = 1972},
                            new {firstname="pierre", lastname="chalamet", birthyear=1973});

            string clusterName = cluster.DescribeClusterName();
            Console.WriteLine(clusterName);

            try
            {


                //Query initSchema = new Query("drop table People; create table People (firstname text primary key) with default_validation=blob");
                //cluster.Execute(initSchema);

                //PreparedQuery insert = new PreparedQuery("insert into People (firstname, birthyear) values (?, ?)");
                //cluster.Execute(insert, new {firstname = "pierre", birthyear = 1973});

                //PreparedQuery select = new PreparedQuery("select birthyear from People where firstname = ?");
                //SelectOutput selectOutput= cluster.Execute<SelectOutput>(select, new {firstname="pierre"});

                // initialize schema
                try
                {
                    cluster.ExecuteCql("drop table People");
                }
                catch
                {
                }
                cluster.ExecuteCql("create table People (firstname text primary key) with default_validation=blob");

                // insert data
                List<byte[]> prms;
                CqlResult result;

                Utf8NameOrValue insert = new Utf8NameOrValue("insert into People (firstname, lastname, birthyear) values (?, ?, ?)");
                CqlPreparedResult preparedInsert = cluster.ExecuteCommand(null, ctx => ctx.CassandraClient.prepare_cql_query(insert.ToByteArray(), Compression.NONE));

                Utf8NameOrValue firstName = new Utf8NameOrValue("pierre");
                Utf8NameOrValue lastName = new Utf8NameOrValue("chalamet");
                IntNameOrValue birthyear = new IntNameOrValue(1973);

                prms = new List<byte[]> {firstName.ToByteArray(), lastName.ToByteArray(), birthyear.ToByteArray()};
                result = cluster.ExecuteCommand(null, ctx => ctx.CassandraClient.execute_prepared_cql_query(preparedInsert.ItemId, prms));

                prms = new List<byte[]> {new Utf8NameOrValue("isabelle").ToByteArray(), lastName.ToByteArray(), new IntNameOrValue(1972).ToByteArray()};
                result = cluster.ExecuteCommand(null, ctx => ctx.CassandraClient.execute_prepared_cql_query(preparedInsert.ItemId, prms));

                //result = cluster.ExecuteCql("select * from People where firstname in (?)");
                //DumpCqlResult(result);

                // query data
                Utf8NameOrValue select = new Utf8NameOrValue("select lastname, birthyear from People where firstname in (?, ?)");
                CqlPreparedResult preparedSelect = cluster.ExecuteCommand(null, ctx => ctx.CassandraClient.prepare_cql_query(select.ToByteArray(), Compression.NONE));

                prms = new List<byte[]> {firstName.ToByteArray(), new Utf8NameOrValue("isabelle").ToByteArray()};
                result = cluster.ExecuteCommand(null, ctx => ctx.CassandraClient.execute_prepared_cql_query(preparedSelect.ItemId, prms));
                DumpCqlResult(result);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            //var nvColumn = new Utf8NameOrValue("column");
            //var nvKey = new Utf8NameOrValue("key");
            //var nvValue = new ByteArrayNameOrValue(new byte[10]);
            //cluster.Insert(columnFamily: "CF",
            //               key: nvKey,
            //               column: nvColumn,
            //               value: nvValue);
        }

        private static void DumpCqlResult(CqlResult result)
        {
            Console.WriteLine("ResultType = {0}", result.Type);
            Console.WriteLine();
            for (int i = 0; i < result.Rows.Count; ++i)
            {
                Console.WriteLine("key = {0}", new Utf8NameOrValue(result.Rows[i].Key).Value);
                for (int j = 0; j < result.Rows[i].Columns.Count; ++j)
                {
                    Utf8NameOrValue column = new Utf8NameOrValue(result.Rows[i].Columns[j].Name);
                    Console.Write("{0} = ", column.Value);
                    switch (column.Value)
                    {
                        case "firstname":
                            Console.WriteLine("{0}", new Utf8NameOrValue(result.Rows[i].Columns[j].Value).Value);
                            break;

                        case "lastname":
                            Console.WriteLine("{0}", new Utf8NameOrValue(result.Rows[i].Columns[j].Value).Value);
                            break;

                        case "birthyear":
                            Console.WriteLine("{0}", new IntNameOrValue(result.Rows[i].Columns[j].Value).Value);
                            break;
                    }
                }
                Console.WriteLine();
            }
        }
    }
}