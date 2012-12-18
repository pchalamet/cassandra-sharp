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

    [Description("set environment variable")]
    internal class Set : CommandBase
    {
        [Description("set tabular result column width")]
        public int? ColWidth { get; set; }

        [Description("enable tabular result mode")]
        public CommandContext.OutputFormatter? Output { get; set; }

        [Description("enable debug log to console")]
        public bool? Log { get; set; }

        public override void Validate()
        {
            if (ColWidth.HasValue && 3 > ColWidth.Value)
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
            if (ColWidth.HasValue)
            {
                CommandContext.ColumnWidth = ColWidth.Value;
            }

            if (Output.HasValue)
            {
                CommandContext.Formatter = Output.Value;
            }

            if (Log.HasValue)
            {
                CommandContext.DebugLog = Log.Value;
            }
        }
    }
}