// cassandra-sharp - a .NET client for Apache Cassandra
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
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using CassandraSharp;
    using CassandraSharp.CQL;
    using CassandraSharp.CQLPropertyBag;

    internal class CqlStatement : CommandBase
    {
        private readonly string _statement;

        public CqlStatement(string statement)
        {
            _statement = statement;
        }

        public override void Execute()
        {
            ExecutionFlags executionFlags = ExecutionFlags.None;
            if (CommandContext.Tracing)
            {
                executionFlags |= ExecutionFlags.Tracing;
            }

            ICqlCommand cmd = CommandContext.Cluster.CreatePropertyBagCommand();
            Task<IList<IDictionary<string, object>>> res = cmd.Execute<IDictionary<string, object>>(_statement, CommandContext.CL, executionFlags).AsFuture();

            // ensure values are sorted accordingly to column name
            var sortedRes = from row in res.Result
                            select new SortedDictionary<string, object>(row);

            CommandContext.ResultWriter.Write(CommandContext.TextWriter, sortedRes);
        }
    }
}