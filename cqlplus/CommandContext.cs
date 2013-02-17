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
    using System.IO;
    using CassandraSharp;

    internal class CommandContext
    {
        public enum OutputFormatter
        {
            Tab,

            KV,

            CSV,
        }

        static CommandContext()
        {
            Reset();
        }

        public static bool Exit { get; set; }

        public static int ColumnWidth { get; set; }

        public static OutputFormatter Formatter { get; set; }

        public static bool DebugLog { get; set; }

        public static bool LastCommandFailed { get; set; }

        public static ICluster Cluster { get; set; }

        public static IResultWriter ResultWriter { get; set; }

        public static TextWriter TextWriter { get; set; }

        public static string OutputFile { get; set; }

        public static bool Tracing { get; set; }

        public static ConsistencyLevel CL { get; set; }

        public static void Reset()
        {
            ColumnWidth = 40;
            Formatter = OutputFormatter.Tab;
            DebugLog = false;
            Tracing = false;
            CL = ConsistencyLevel.QUORUM;
        }
    }
}