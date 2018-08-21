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

using System.Collections.Generic;
using System.IO;
using System.Text;
using CassandraSharp.CQLPropertyBag;

namespace cqlplus.ResultWriter
{
    internal class CSV : IResultWriter
    {
        public void Write(TextWriter txtWriter, IEnumerable<PropertyBag> rowSet)
        {
            var first = true;
            foreach (var row in rowSet)
            {
                string sep;
                if (first)
                {
                    var sbHeader = new StringBuilder();
                    sep = string.Empty;
                    foreach (var col in row.Keys)
                    {
                        sbHeader.AppendFormat("{0}{1}", sep, col);
                        sep = ",";
                    }

                    var header = sbHeader.ToString();
                    txtWriter.WriteLine(header);
                    first = false;
                }

                var sbValues = new StringBuilder();
                sep = string.Empty;
                foreach (var col in row.Keys)
                {
                    sbValues.AppendFormat("{0}{1}", sep, ValueFormatter.Format(row[col]));
                    sep = ",";
                }

                var values = sbValues.ToString();
                txtWriter.WriteLine(values);
            }
        }
    }
}