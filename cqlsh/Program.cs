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

namespace cqlsh
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using CassandraSharp;
    using CassandraSharp.CQLPropertyBag;
    using CassandraSharp.Config;
    using cqlsh.ResultWriter;
    using cqlsh.StatementReader;

    internal class Program
    {
        private static IResultWriter _display;

        private static CliArgs _cliArgs;

        private static int Main(string[] args)
        {
            _cliArgs = new CliArgs();
            if (!Parser.ParseArguments(args, _cliArgs))
            {
                string usage = Parser.ArgumentsUsage(typeof(CliArgs));
                Console.WriteLine(usage);
                return 5;
            }

            _display = new Tabular(15);

            string hostname = _cliArgs.Hostname;
            IStatementReader statementInput = new ConsoleInput();
            if (null != _cliArgs.File)
            {
                statementInput = new FileInput(_cliArgs.File);
            }

            try
            {
                var statementReader = new StatementSplitter(statementInput);
                Run(hostname, statementReader);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed with error {0}", ex);
                return 5;
            }

            return 0;
        }

        private static void Run(string hostname, IStatementReader statementReader)
        {
            CassandraSharpConfig cassandraSharpConfig = new CassandraSharpConfig();
            ClusterManager.Configure(cassandraSharpConfig);

            ClusterConfig clusterConfig = new ClusterConfig
                {
                        BehaviorConfig = null,
                        Endpoints = new EndpointsConfig
                            {
                                    Servers = new[] {hostname}
                            }
                };

            using (ICluster cluster = ClusterManager.GetCluster(clusterConfig))
            {
                if (_cliArgs.CheckConnection)
                {
                    const string checkStatement = "select cluster_name, data_center, rack, release_version from system.local";
                    ExecuteCQLStatement(cluster, checkStatement, new Tabular());
                }

                foreach (string statement in statementReader.Read())
                {
                    ExecuteCQLStatement(cluster, statement, _display);
                }
            }
        }

        private static void ExecuteCQLStatement(ICluster cluster, string statement, IResultWriter resultWriter)
        {
            try
            {
                Task<IEnumerable<Dictionary<string, object>>> res = cluster.Execute(statement, ConsistencyLevel.QUORUM);
                resultWriter.Write(res.Result);
            }
            catch (Exception ex)
            {
                if (null != ex.InnerException)
                {
                    ex = ex.InnerException;
                }

                Console.WriteLine("Command failed with error: {0}", ex.Message);
            }
        }
    }
}