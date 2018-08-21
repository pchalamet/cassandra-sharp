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

namespace cqlplus
{
    using System;
    using CassandraSharp;
    using CassandraSharp.Config;
    using cqlplus.Commands;
    using cqlplus.StatementReader;

    internal class Program
    {
        private static CliArgs _cliArgs;

        private static int Main(string[] args)
        {
            _cliArgs = new CliArgs();
            if (!CommandLineParser.ParseArguments(args, _cliArgs))
            {
                string usage = CommandLineParser.ArgumentsUsage(typeof(CliArgs));
                Console.WriteLine(usage);
                return 5;
            }

            IStatementReader statementInput = new ConsoleInput(_cliArgs.Hostname);
            if (null != _cliArgs.File)
            {
                statementInput = new FileInput(_cliArgs.File);
            }

            CommandContext.DebugLog = _cliArgs.DebugLog;

            try
            {
                var statementReader = new StatementSplitter(statementInput);
                Run(statementReader);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed with error {0}", ex);
                return 5;
            }

            return 0;
        }

        private static void Run(IStatementReader statementReader)
        {
            CassandraSharpConfig cassandraSharpConfig = new CassandraSharpConfig
            {
                Logger = new LoggerConfig { Type = typeof(ConsoleDebugLogger).AssemblyQualifiedName },
                Instrumentation = new InstrumentationConfig { Type = typeof(ConsoleInstrumentation).AssemblyQualifiedName }
            };

            using (var clusterManager = new ClusterManager(cassandraSharpConfig))
            {
                ClusterConfig clusterConfig = new ClusterConfig
                {
                    Transport = new TransportConfig
                    {
                        User = _cliArgs.User,
                        Password = _cliArgs.Password
                    },
                    Endpoints = new EndpointsConfig
                    {
                        Servers = new[] { _cliArgs.Hostname },
                    }
                };
                if (!_cliArgs.Discovery)
                {
                    clusterConfig.Endpoints.Discovery = new DiscoveryConfig { Type = "Null" };
                }

                using (ICluster cluster = clusterManager.GetCluster(clusterConfig))
                {
                    CommandContext.Cluster = cluster;

                    if (_cliArgs.CheckConnection)
                    {
                        Console.WriteLine("Connecting to {0}:{1}...", _cliArgs.Hostname, _cliArgs.Port);
                        const string checkStatement = "select cluster_name, data_center, rack, release_version from system.local";
                        new Exec { Statement = checkStatement }.Execute();
                        if (CommandContext.LastCommandFailed)
                        {
                            return;
                        }

                        Console.WriteLine();
                        Console.WriteLine("Querying ring state...");
                        const string peersStatement = "select rpc_address,tokens,release_version from system.peers";
                        new Exec { Statement = peersStatement }.Execute();
                        Console.WriteLine();

                        if (CommandContext.LastCommandFailed)
                        {
                            return;
                        }
                    }

                    if (!_cliArgs.NoHelp)
                    {
                        new Exec { Statement = "!help" }.Execute();
                    }

                    foreach (string statement in statementReader.Read())
                    {
                        new Exec { Statement = statement }.Execute();
                        if (CommandContext.Exit)
                        {
                            return;
                        }
                    }
                }
            }
        }
    }
}