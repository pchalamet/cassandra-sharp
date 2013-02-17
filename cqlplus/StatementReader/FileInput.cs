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

namespace cqlplus.StatementReader
{
    using System.Collections.Generic;
    using System.IO;

    public class FileInput : IStatementReader
    {
        private readonly string _filename;

        public FileInput(string filename)
        {
            _filename = filename;
        }

        public IEnumerable<string> Read()
        {
            using (TextReader txtReader = new StreamReader(_filename))
            {
                string line = txtReader.ReadLine();
                while (null != line)
                {
                    yield return line;
                    line = txtReader.ReadLine();
                }
            }
        }
    }
}