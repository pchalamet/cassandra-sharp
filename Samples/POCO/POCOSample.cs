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

namespace Samples.POCO
{
    using System;
    using System.Collections.Generic;
    using CassandraSharp;
    using CassandraSharp.CQL;
    using CassandraSharp.CQLPoco;
    using CassandraSharp.Config;

    public class NerdMovie
    {
        public string Director;

        public string MainActor;

        public string Movie;

        public int Year;
    }

    public class POCOSample : Sample
    {
        public POCOSample()
                : base("POCOSample")
        {
        }

        protected override void InternalRun()
        {
            XmlConfigurator.Configure();
            using (ICluster cluster = ClusterManager.GetCluster("TestCassandra"))
            {
                const string createKeyspace = "CREATE KEYSPACE videos WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
                Console.WriteLine("============================================================");
                Console.WriteLine(createKeyspace);
                Console.WriteLine("============================================================");

                var resCount = cluster.ExecuteNonQuery(createKeyspace);
                resCount.Wait();
                Console.WriteLine();
                Console.WriteLine();

                const string createNerdMovies = "CREATE TABLE videos.NerdMovies (movie text, " +
                                                "director text, " +
                                                "main_actor text, " +
                                                "year int, " +
                                                "PRIMARY KEY (movie, director))";
                Console.WriteLine("============================================================");
                Console.WriteLine(createNerdMovies);
                Console.WriteLine("============================================================");
                resCount = cluster.ExecuteNonQuery(createNerdMovies);
                resCount.Wait();
                Console.WriteLine();
                Console.WriteLine();

                const string insertNerdMovie = "INSERT INTO videos.NerdMovies (movie, director, main_actor, year)" +
                                               "VALUES ('Serenity', 'Joss Whedon', 'Nathan Fillion', 2005) " +
                                               "USING TTL 86400";
                resCount = cluster.ExecuteNonQuery(insertNerdMovie);
                resCount.Wait();
                Console.WriteLine();
                Console.WriteLine();

                const string selectNerdMovies = "select * from videos.NerdMovies";
                Console.WriteLine("============================================================");
                Console.WriteLine(selectNerdMovies);
                Console.WriteLine("============================================================");
                var taskSelectStartMovies = cluster.Execute<NerdMovie>(selectNerdMovies).ContinueWith(res => DisplayMovies(res.Result));
                taskSelectStartMovies.Wait();

                const string selectAllFrom = "select * from videos.NerdMovies where director=? ALLOW FILTERING";
                Console.WriteLine("============================================================");
                Console.WriteLine(selectAllFrom);
                Console.WriteLine("============================================================");
                var preparedAllFrom = cluster.Prepare(selectAllFrom);
                var ds = new {Director = "Joss Whedon"};
                var taskSelectWhere =
                        preparedAllFrom.Execute<NerdMovie>(ds).ContinueWith(res => DisplayMovies(res.Result));
                taskSelectWhere.Wait();

                const string dropExcelsor = "drop keyspace videos";
                Console.WriteLine("============================================================");
                Console.WriteLine(dropExcelsor);
                Console.WriteLine("============================================================");
                var taskDrop = cluster.ExecuteNonQuery(dropExcelsor);
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
    }
}