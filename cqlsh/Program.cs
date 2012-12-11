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
    using System.IO;
    using System.Threading.Tasks;
    using CassandraSharp;
    using CassandraSharp.CQL;
    using CassandraSharp.CQLPropertyBag;
    using CassandraSharp.Config;
    using CommandLine;

    internal class Program
    {
        private static int Main(string[] args)
        {
            CliArgs cliArgs = new CliArgs();
            if (! Parser.ParseArguments(args, cliArgs))
            {
                string usage = Parser.ArgumentsUsage(typeof(CliArgs));
                Console.WriteLine(usage);
                return 5;
            }

            string hostname = cliArgs.hostname;
            Func<IEnumerable<string>> lineReader = ConsoleReader;
            if (null != cliArgs.file)
            {
                lineReader = () => FileReader(cliArgs.file);
            }

            try
            {
                var statementReader = StatementReader(lineReader());
                Run(hostname, statementReader);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed with error {0}", ex);
                return 5;
            }

            return 0;
        }

        private static void Run(string hostname, IEnumerable<string> statements)
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
                foreach (string statement in statements)
                {
                    Task<IList<Dictionary<string, object>>> futRes = cluster.Execute(statement, ConsistencyLevel.QUORUM).AsFuture();
                    try
                    {
                        int rowNum = 0;
                        foreach (var row in futRes.Result)
                        {
                            Console.Write("{0,-2}: ", rowNum++);
                            string offset = "";
                            foreach (var col in row)
                            {
                                Console.WriteLine("{0}{1} : {2} ", offset, col.Key, col.Value);
                                offset = "    ";
                            }
                            Console.WriteLine();
                        }
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

        private static IEnumerable<string> StatementReader(IEnumerable<string> lineReader)
        {
            string statement = "";
            int semiColomnIdx;
            foreach (string line in lineReader)
            {
                statement += line + " ";
                semiColomnIdx = statement.IndexOf(';');
                while (-1 != semiColomnIdx)
                {
                    string singleStatement = statement.Substring(0, semiColomnIdx).Trim();
                    if (0 < singleStatement.Length)
                    {
                        yield return singleStatement;
                    }

                    statement = statement.Substring(semiColomnIdx + 1).Trim();
                    semiColomnIdx = statement.IndexOf(';');
                }
            }

            semiColomnIdx = statement.IndexOf(';');
            while (-1 != semiColomnIdx)
            {
                string singleStatement = statement.Substring(0, semiColomnIdx).Trim();
                if (0 < singleStatement.Length)
                {
                    yield return singleStatement;
                }

                statement = statement.Substring(semiColomnIdx + 1).Trim();
                semiColomnIdx = statement.IndexOf(';');
            }

            if (0 < statement.Length)
            {
                yield return statement;
            }
        }

        private static IEnumerable<string> FileReader(string file)
        {
            using (TextReader txtReader = new StreamReader(file))
            {
                string line = txtReader.ReadLine();
                while (null != line)
                {
                    yield return line;
                    line = txtReader.ReadLine();
                }
            }
        }

        private static IEnumerable<string> ConsoleReader()
        {
            while (true)
            {
                Console.Write("cqlsh> ");
                string line = Console.ReadLine();
                yield return line;
            }
// ReSharper disable FunctionNeverReturns
        }
// ReSharper restore FunctionNeverReturns
    }
}