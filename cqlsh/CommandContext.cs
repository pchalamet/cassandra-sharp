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
    using CassandraSharp;

    internal class CommandContext
    {
        public static CommandContext Instance = new CommandContext();

        public CommandContext()
        {
            ColumnWidth = 15;
            Tabular = true;
        }

        public bool Exit { get; set; }

        public int ColumnWidth { get; set; }

        public bool Tabular { get; set; }

        public ICluster Cluster { get; set; }

        public IResultWriter ResultWriter { get; set; }
    }
}