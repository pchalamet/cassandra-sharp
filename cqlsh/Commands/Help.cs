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

namespace cqlsh.Commands
{
    using System;
    using System.Collections.Generic;

    internal class Help : ICommand
    {
        public string Describe()
        {
            return "display help";
        }

        public void Execute()
        {
            int cmdMaxLen = 0;
            foreach (var cmdType in GenericCommand.GetRegisteredCommands())
            {
                cmdMaxLen = Math.Max(cmdMaxLen, cmdType.Key.Length);
            }

            Console.WriteLine("Commands:");
            string format = string.Format("  !{{0,-{0}}} - ", cmdMaxLen);
            foreach (var cmdType in GenericCommand.GetRegisteredCommands())
            {
                ICommand cmd = (ICommand) Activator.CreateInstance(cmdType.Value);
                string description = cmd.Describe();
                string startOfLine = string.Format(format, cmdType.Key);
                string nextStartOfLine = new string(' ', startOfLine.Length);
                string[] lines = description.Split(new[] {"\n"}, StringSplitOptions.RemoveEmptyEntries);
                foreach (string line in lines)
                {
                    Console.WriteLine("{0}{1}", startOfLine, line);
                    startOfLine = nextStartOfLine;
                }
            }
            Console.WriteLine("  CQL query");

            Console.WriteLine();
            Console.WriteLine("Examples:");
            Console.WriteLine("  !set log=true colwidth=20;");
            Console.WriteLine("  select * from system.local;");
        }
    }
}