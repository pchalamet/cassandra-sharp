// cassandra-sharp - the high performance .NET CQL 3 binary protocol client for Apache Cassandra
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

    public class NerdMovie
    {
        public string Director;

        public string MainActor;

        public string Movie;

        public int Year;
    }

    public class PocoSample : Sample
    {
        public PocoSample()
                : base("POCOSample")
        {
        }

        protected override void CreateKeyspace(ICluster cluster)
        {
            ICqlCommand cmd = cluster.CreatePocoCommand();

            const string createKeyspace = "CREATE KEYSPACE videos WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
            cmd.Execute(createKeyspace).AsFuture().Wait();

            const string createNerdMovies = "CREATE TABLE videos.NerdMovies (movie text, " +
                                            "director text, " +
                                            "main_actor text, " +
                                            "year int, " +
                                            "PRIMARY KEY (movie, director))";
            cmd.Execute(createNerdMovies).AsFuture().Wait();
        }

        protected override void DropKeyspace(ICluster cluster)
        {
            ICqlCommand cmd = cluster.CreatePocoCommand();

            const string dropExcelsor = "drop keyspace videos";
            cmd.Execute(dropExcelsor).AsFuture().Wait();
        }

        protected override void InternalRun(ICluster cluster)
        {
            ICqlCommand cmd = cluster.CreatePocoCommand();

            const string insertNerdMovie = "INSERT INTO videos.NerdMovies (movie, director, main_actor, year)" +
                                           "VALUES ('Serenity', 'Joss Whedon', 'Nathan Fillion', 2005) " +
                                           "USING TTL 86400";
            Console.WriteLine(insertNerdMovie);
            cmd.Execute(insertNerdMovie).AsFuture().Wait();
            Console.WriteLine();

            const string selectNerdMovies = "select * from videos.NerdMovies";
            Console.WriteLine(selectNerdMovies);
            var taskSelectStartMovies = cmd.Execute<NerdMovie>(selectNerdMovies).AsFuture().ContinueWith(res => DisplayMovies(res.Result));
            taskSelectStartMovies.Wait();
            Console.WriteLine();

            const string selectAllFrom = "select * from videos.NerdMovies where director=? ALLOW FILTERING";
            Console.WriteLine(selectAllFrom);
            var preparedAllFrom = cmd.Prepare<NerdMovie>(selectAllFrom);
            var ds = new {Director = "Joss Whedon"};
            var taskSelectWhere =
                    preparedAllFrom.Execute(ds).AsFuture().ContinueWith(res => DisplayMovies(res.Result));
            taskSelectWhere.Wait();
            Console.WriteLine();
        }

        private static void DisplayMovies(IEnumerable<NerdMovie> result)
        {
            foreach (var resMovie in result)
            {
                Console.WriteLine("Movie={0} Director={1} MainActor={2}, Year={3}",
                                  resMovie.Movie, resMovie.Director, resMovie.MainActor, resMovie.Year);
            }
        }
    }
}