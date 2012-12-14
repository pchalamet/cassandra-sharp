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

    internal class Help : ICommand
    {
        public void Execute()
        {
            Console.WriteLine("Available commands:");
            Console.WriteLine("  help            - display help");
            Console.WriteLine("  exit            - exit cqlsh");
            Console.WriteLine("  reset           - reset environment");
            Console.WriteLine("  tab=<bool>      - enable tabular result output");
            Console.WriteLine("  colwidth=<int>  - set column width for tabular result output");
            Console.WriteLine("  log=<bool>      - enable debug logger");
            Console.WriteLine();
            Console.WriteLine("Statements end with ';'");
            Console.WriteLine("  - Commands start with '!'");
            Console.WriteLine("  - CQL command are entered as is");
        }
    }
}