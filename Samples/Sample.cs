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

namespace Samples
{
    using System;
    using CassandraSharp;
    using CassandraSharp.Config;

    public abstract class Sample
    {
        private readonly string _name;

        protected Sample(string name)
        {
            _name = name;
        }

        public void Run()
        {
            Console.WriteLine("=======================================================");
            Console.WriteLine("== RUNNING     {0}", _name);
            Console.WriteLine("=======================================================");

            XmlConfigurator.Configure();
            using (var clusterManager = new ClusterManager())
            using (ICluster cluster = clusterManager.GetCluster("TestCassandra"))
            {
                try
                {
                    DropKeyspace(cluster);
                }
                // ReSharper disable EmptyGeneralCatchClause
                catch
                // ReSharper restore EmptyGeneralCatchClause
                {
                }

                CreateKeyspace(cluster);

                try
                {
                    InternalRun(cluster);
                    DropKeyspace(cluster);
                }
                catch (Exception ex)
                {
                    string msg = string.Format("== FAILED  with error\n{0}", ex);
                    Console.WriteLine("=======================================================");
                    Console.WriteLine(msg);
                    Console.WriteLine("=======================================================");
                }

                Console.WriteLine();
                Console.WriteLine();
                Console.WriteLine();
            }
        }

        protected virtual void CreateKeyspace(ICluster cluster)
        {
        }

        protected abstract void InternalRun(ICluster cluster);

        protected virtual void DropKeyspace(ICluster cluster)
        {
        }
    }
}