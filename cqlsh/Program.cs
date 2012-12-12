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
    using System.Text;
    using System.Threading.Tasks;
    using CassandraSharp;
    using CassandraSharp.CQLPropertyBag;
    using CassandraSharp.Config;
    using CommandLine;

    internal class Program
    {
        private static Action<IEnumerable<Dictionary<string, object>>> _display = DisplayTabularResult;

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
                    //if (statement.StartsWith("!"))
                    //{
                    //    string[] items = statement.Split(' ');
                    //    if (items[0] == "!table")
                    //    {
                    //        if (items[1] == "off")
                    //        {
                    //            _display = DisplayRowContent;
                    //        }
                    //        else
                    //        {
                    //            _display = DisplayTabularResult;
                    //        }
                    //    }
                    //}
                    //else
                    {
                        ExecuteCQLStatement(cluster, statement, _display);
                    }
                    Console.WriteLine();
                }
            }
        }

        private static void ExecuteCQLStatement(ICluster cluster, string statement, Action<IEnumerable<Dictionary<string, object>>> display)
        {
            try
            {
                Task<IEnumerable<Dictionary<string, object>>> res = cluster.Execute(statement, ConsistencyLevel.QUORUM);
                display(res.Result);
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

        private static void DisplayRowContent(IEnumerable<Dictionary<string, object>> rowSet)
        {
            int rowNum = 0;
            foreach (var row in rowSet)
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

        private static void DisplayTabularResult(IEnumerable<Dictionary<string, object>> rowSet)
        {
            const int maxWidth = 20;
            string colFormat = string.Format("{{0,-{0}}}", maxWidth);
            string rowSeparator = null;
            foreach (Dictionary<string, object> row in rowSet)
            {
                if (null == row)
                {
                    continue;
                }

                if (null == rowSeparator)
                {
                    StringBuilder sbHeader = new StringBuilder();
                    StringBuilder sbRowSeparator = new StringBuilder();
                    foreach (KeyValuePair<string, object> value in row)
                    {
                        string colHeader = string.Format(colFormat, value.Key);
                        if (maxWidth < colHeader.Length)
                        {
                            colHeader = colHeader.Substring(0, maxWidth - 1);
                            colHeader += "~";
                        }
                        sbHeader.Append("| ").Append(colHeader).Append(" ");
                        sbRowSeparator.Append("+-").Append(new string('-', colHeader.Length)).Append('-');
                    }
                    sbHeader.Append(" |");
                    sbRowSeparator.Append("-+");

                    string header = sbHeader.ToString();
                    rowSeparator = sbRowSeparator.ToString();
                    string headerSeparator = rowSeparator.Replace('-', '=');

                    Console.WriteLine(rowSeparator);
                    Console.WriteLine(header);
                    Console.WriteLine(headerSeparator);
                }

                StringBuilder sbValues = new StringBuilder();
                foreach (KeyValuePair<string, object> value in row)
                {
                    string colValue = string.Format(colFormat, value.Value);
                    if (maxWidth < colValue.Length)
                    {
                        colValue = colValue.Substring(0, maxWidth - 1);
                        colValue += "~";
                    }
                    sbValues.Append("| ").Append(colValue).Append(" ");
                }
                sbValues.Append(" |");
                string rowValues = sbValues.ToString();

                Console.WriteLine(rowValues);
                Console.WriteLine(rowSeparator);
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