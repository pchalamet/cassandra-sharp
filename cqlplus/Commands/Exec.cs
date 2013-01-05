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

namespace cqlplus.Commands
{
    using System;
    using cqlplus.Parser;
    using cqlplus.ResultWriter;

    [Description("execute statement")]
    internal class Exec : CommandBase
    {
        [Description("Statement string", Mandatory = true)]
        public string Statement { get; set; }

        public override void Execute()
        {
            try
            {
                CommandContext.LastCommandFailed = false;

                Scanner scanner = new Scanner();
                Parser parser = new Parser(scanner);
                ParseTree parseTree = parser.Parse(Statement);
                if (0 < parseTree.Errors.Count)
                {
                    throw new ArgumentException(parseTree.Errors[0].Message);
                }

                object result = parseTree.Eval(null);
                ICommand command = (ICommand) result;

                CommandContext.ResultWriter = GetResultWriter();
                CommandContext.TextWriter = Console.Out;
                command.Validate();
                command.Execute();
            }
            catch (Exception ex)
            {
                CommandContext.LastCommandFailed = true;

                if (CommandContext.DebugLog)
                {
                    Console.WriteLine("Command execution failed with error:\n{0}", ex);
                }
                else
                {
                    while (null != ex.InnerException)
                    {
                        ex = ex.InnerException;
                    }

                    Console.WriteLine("Command execution failed with error '{0}'", ex.Message);
                }
            }
        }

        private static IResultWriter GetResultWriter()
        {
            switch (CommandContext.Formatter)
            {
                case CommandContext.OutputFormatter.Tab:
                    return new Tabular(CommandContext.ColumnWidth);

                case CommandContext.OutputFormatter.KV:
                    return new RowKeyValue();

                case CommandContext.OutputFormatter.CSV:
                    return new CSV();

                default:
                    throw new ArgumentException("Unknown formatter");
            }
        }
    }
}