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
    using cqlplus.StatementReader;

    [Description("load statements from specific file")]
    internal class Source : CommandBase
    {
        [Description("File containing statements", Mandatory = true)]
        public string File { get; set; }

        public override void Execute()
        {
            IStatementReader statementReader = new FileInput(File);
            statementReader = new StatementSplitter(statementReader);
            foreach (string statement in statementReader.Read())
            {
                new Exec {Statement = statement}.Execute();

                if (CommandContext.Exit || CommandContext.LastCommandFailed)
                {
                    return;
                }
            }
        }
    }
}