// cassandra-sharp - the high performance .NET CQL 3 binary protocol client for Apache Cassandra
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

namespace cqlplus.Commands
{
    using System;
    using CassandraSharp;

    [Description("set environment variable")]
    internal class Set : CommandBase
    {
        [Description("set max result column width")]
        public int? MaxWidth { get; set; }

        [Description("select output formatter")]
        public CommandContext.OutputFormatter? Output { get; set; }

        [Description("enable debug log to console")]
        public bool? Log { get; set; }

        [Description("enable query tracing")]
        public bool? Tracing { get; set; }

        [Description("consistency level")]
        public ConsistencyLevel? CL { get; set; }

        public override void Validate()
        {
            if (MaxWidth.HasValue && 3 > MaxWidth.Value)
            {
                throw new ArgumentException("ColWidth must be greater than 3");
            }

            if (Output.HasValue && !Enum.IsDefined(typeof(CommandContext.OutputFormatter), Output.Value))
            {
                throw new ArgumentException("Unknown value for OutputFormatter");
            }
        }

        public override void Execute()
        {
            if (MaxWidth.HasValue)
            {
                CommandContext.ColumnWidth = MaxWidth.Value;
            }

            if (Output.HasValue)
            {
                CommandContext.Formatter = Output.Value;
            }

            if (Log.HasValue)
            {
                CommandContext.DebugLog = Log.Value;
            }

            if (Tracing.HasValue)
            {
                CommandContext.Tracing = Tracing.Value;
            }

            if (CL.HasValue)
            {
                CommandContext.CL = CL.Value;
            }
        }
    }
}