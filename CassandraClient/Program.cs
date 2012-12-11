// cassandra-sharp - a .NET client for Apache Cassandra
// Copyright (c) 2011-2012 Pierre Chalamet
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

namespace CassandraClient
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using CassandraSharp;
    using CassandraSharp.CQL;
    using CassandraSharp.CQLPoco;
    using CassandraSharp.Config;

    public class SchemaKeyspaces
    {
        public bool DurableWrites { get; set; }

        public string KeyspaceName { get; set; }

// ReSharper disable InconsistentNaming
        public string strategy_Class { get; set; }
// ReSharper restore InconsistentNaming

// ReSharper disable InconsistentNaming
        public string strategy_options { get; set; }
// ReSharper restore InconsistentNaming
    }

    public class NerdMovie
    {
        public string Director;

        public string MainActor;

        public string Movie;

        public int Year;
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Running sample");
            Sample.Sample.QueryKeyspaces().Wait();

            Console.WriteLine("Running main");
            XmlConfigurator.Configure();
            using (ICluster cluster = ClusterManager.GetCluster("TestCassandra"))
            {
                const string cqlKeyspaces = "SELECT * from system.schema_keyspaces";
                Console.WriteLine("============================================================");
                Console.WriteLine(cqlKeyspaces);
                Console.WriteLine("============================================================");

                var resKeyspaces = cluster.Execute<SchemaKeyspaces>(cqlKeyspaces, ConsistencyLevel.QUORUM).ContinueWith(t => DisplayKeyspace(t.Result));
                var resKeyspaces2 = cluster.Execute<SchemaKeyspaces>(cqlKeyspaces, ConsistencyLevel.QUORUM).ContinueWith(t => DisplayKeyspace(t.Result));
                resKeyspaces.Wait();
                resKeyspaces2.Wait();

                List<Task<IList<SchemaKeyspaces>>> results = new List<Task<IList<SchemaKeyspaces>>>();
                for (int i = 0; i < 10; ++i)
                {
                    Task<IList<SchemaKeyspaces>> futKeyspaces1 = cluster.Execute<SchemaKeyspaces>(cqlKeyspaces, ConsistencyLevel.QUORUM).AsFuture();
                    Task<IList<SchemaKeyspaces>> futKeyspaces2 = cluster.Execute<SchemaKeyspaces>(cqlKeyspaces, ConsistencyLevel.QUORUM).AsFuture();
                    results.Add(futKeyspaces1);
                    results.Add(futKeyspaces2);
                }
                foreach (var result in results)
                {
                    DisplayKeyspace(result.Result);
                }

                const string createExcelsior = "CREATE KEYSPACE Excelsior WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
                Console.WriteLine("============================================================");
                Console.WriteLine(createExcelsior);
                Console.WriteLine("============================================================");

                var resCount = cluster.ExecuteNonQuery(createExcelsior, ConsistencyLevel.QUORUM);
                resCount.Wait();
                Console.WriteLine();
                Console.WriteLine();

                const string createNerdMovies = "CREATE TABLE Excelsior.NerdMovies (movie text, " +
                                                "director text, " +
                                                "main_actor text, " +
                                                "year int, " +
                                                "PRIMARY KEY (movie, director))";
                Console.WriteLine("============================================================");
                Console.WriteLine(createNerdMovies);
                Console.WriteLine("============================================================");
                resCount = cluster.ExecuteNonQuery(createNerdMovies, ConsistencyLevel.QUORUM);
                resCount.Wait();
                Console.WriteLine();
                Console.WriteLine();

                const string insertNerdMovie = "INSERT INTO Excelsior.NerdMovies (movie, director, main_actor, year)" +
                                               "VALUES ('Serenity', 'Joss Whedon', 'Nathan Fillion', 2005) " +
                                               "USING TTL 86400";
                resCount = cluster.ExecuteNonQuery(insertNerdMovie, ConsistencyLevel.QUORUM);
                resCount.Wait();
                Console.WriteLine();
                Console.WriteLine();

                const string selectNerdMovies = "select * from excelsior.NerdMovies";
                Console.WriteLine("============================================================");
                Console.WriteLine(selectNerdMovies);
                Console.WriteLine("============================================================");
                var taskSelectStartMovies = cluster.Execute<NerdMovie>(selectNerdMovies, ConsistencyLevel.QUORUM).ContinueWith(res => DisplayMovies(res.Result));
                taskSelectStartMovies.Wait();

                const string selectAllFrom = "select * from excelsior.NerdMovies where director=? ALLOW FILTERING";
                Console.WriteLine("============================================================");
                Console.WriteLine(selectAllFrom);
                Console.WriteLine("============================================================");
                var preparedAllFrom = cluster.Prepare(selectAllFrom);
                var taskSelectWhere =
                        preparedAllFrom.Result.Execute<NerdMovie>(ConsistencyLevel.QUORUM, "Joss Whedon").ContinueWith(res => DisplayMovies(res.Result));
                taskSelectWhere.Wait();

                const string dropExcelsor = "drop keyspace excelsior";
                Console.WriteLine("============================================================");
                Console.WriteLine(dropExcelsor);
                Console.WriteLine("============================================================");
                var taskDrop = cluster.ExecuteNonQuery(dropExcelsor, ConsistencyLevel.QUORUM);
                taskDrop.Wait();
                Console.WriteLine();
                Console.WriteLine();
            }

            ClusterManager.Shutdown();
        }

        private static void DisplayMovies(IEnumerable<NerdMovie> result)
        {
            foreach (var resMovie in result)
            {
                Console.WriteLine("Movie={0} Director={1} MainActor={2}, Year={3}",
                                  resMovie.Movie, resMovie.Director, resMovie.MainActor, resMovie.Year);
            }
            Console.WriteLine();
            Console.WriteLine();
        }

        private static void DisplayKeyspace(IEnumerable<SchemaKeyspaces> result)
        {
            try
            {
                foreach (var resKeyspace in result)
                {
                    Console.WriteLine("DurableWrites={0} KeyspaceName={1} strategy_Class={2} strategy_options={3}",
                                      resKeyspace.DurableWrites, resKeyspace.KeyspaceName, resKeyspace.strategy_Class, resKeyspace.strategy_options);
                }
                Console.WriteLine();
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Command failed {0}", ex.Message);
            }
        }
    }
}