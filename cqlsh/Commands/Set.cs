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
    using System.Text;

    internal class Set : ICommand
    {
        public int? ColWidth { get; set; }

        public bool? Tab { get; set; }

        public bool? Log { get; set; }

        public string Describe()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("set environment variable");
            sb.AppendLine("-> ColWidth=<int> : set tabular result column width");
            sb.AppendLine("-> Tab=<bool> : enable tabular result mode");
            sb.AppendLine("-> Log=<bool> : enable debug log to console");

            return sb.ToString();
        }

        public void Execute()
        {
            if (ColWidth.HasValue)
            {
                CommandContext.ColumnWidth = ColWidth.Value;
            }

            if (Tab.HasValue)
            {
                CommandContext.Tabular = Tab.Value;
            }

            if (Log.HasValue)
            {
                CommandContext.DebugLog = Log.Value;
            }
        }
    }
}