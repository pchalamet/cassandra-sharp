// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2018 Pierre Chalamet
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

namespace cqlplus.ResultWriter
{
    using System.Collections.Generic;
    using System.IO;
    using CassandraSharp.CQLPropertyBag;

    public class RowKeyValue : IResultWriter
    {
        public void Write(TextWriter txtWriter, IEnumerable<PropertyBag> rowSet)
        {
            int rowNum = 0;
            foreach (var row in rowSet)
            {
                txtWriter.Write("{0,-2}: ", rowNum++);
                string offset = "";
                foreach (var col in row.Keys)
                {
                    string sValue = ValueFormatter.Format(row[col]);
                    txtWriter.WriteLine("{0}{1} : {2} ", offset, col, sValue);
                    offset = "    ";
                }
                txtWriter.WriteLine();
            }
        }
    }
}