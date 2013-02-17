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

namespace cqlplus.StatementReader
{
    using System.Collections.Generic;

    internal class StatementSplitter : IStatementReader
    {
        private readonly IStatementReader _statementReader;

        public StatementSplitter(IStatementReader statementReader)
        {
            _statementReader = statementReader;
        }

        public IEnumerable<string> Read()
        {
            string statement = "";
            int semiColomnIdx;
            foreach (string line in _statementReader.Read())
            {
                statement += line + " ";
                semiColomnIdx = statement.IndexOf(';');
                while (-1 != semiColomnIdx)
                {
                    string singleStatement = statement.Substring(0, semiColomnIdx).Trim();
                    if (0 < singleStatement.Length)
                    {
                        yield return singleStatement;
                    }

                    statement = statement.Substring(semiColomnIdx + 1).Trim();
                    semiColomnIdx = statement.IndexOf(';');
                }
            }

            semiColomnIdx = statement.IndexOf(';');
            while (-1 != semiColomnIdx)
            {
                string singleStatement = statement.Substring(0, semiColomnIdx).Trim();
                if (0 < singleStatement.Length)
                {
                    yield return singleStatement;
                }

                statement = statement.Substring(semiColomnIdx + 1).Trim();
                semiColomnIdx = statement.IndexOf(';');
            }

            if (0 < statement.Length)
            {
                yield return statement;
            }
        }
    }
}