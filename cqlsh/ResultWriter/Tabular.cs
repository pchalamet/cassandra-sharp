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

namespace cqlsh.ResultWriter
{
    using System.Collections.Generic;
    using System.IO;
    using System.Text;

    public class Tabular : IResultWriter
    {
        private readonly int _maxWidth;

        public Tabular()
                : this(20)
        {
        }

        public Tabular(int maxWidth)
        {
            _maxWidth = maxWidth;
        }

        public void Write(TextWriter txtWriter, IEnumerable<Dictionary<string, object>> rowSet)
        {
            string colFormat = string.Format("{{0,-{0}}}", _maxWidth);
            string rowSeparator = null;
            foreach (Dictionary<string, object> row in rowSet)
            {
                if (null == row)
                {
                    continue;
                }

                if (null == rowSeparator)
                {
                    StringBuilder sbHeader = new StringBuilder();
                    StringBuilder sbRowSeparator = new StringBuilder();
                    foreach (KeyValuePair<string, object> value in row)
                    {
                        string colHeader = string.Format(colFormat, value.Key);
                        if (_maxWidth < colHeader.Length)
                        {
                            colHeader = colHeader.Substring(0, _maxWidth - 1);
                            colHeader += "~";
                        }
                        sbHeader.Append("| ").Append(colHeader).Append(" ");
                        sbRowSeparator.Append("+-").Append(new string('-', colHeader.Length)).Append('-');
                    }
                    sbHeader.Append(" |");
                    sbRowSeparator.Append("-+");

                    string header = sbHeader.ToString();
                    rowSeparator = sbRowSeparator.ToString();
                    string headerSeparator = rowSeparator.Replace('-', '=');

                    txtWriter.WriteLine(rowSeparator);
                    txtWriter.WriteLine(header);
                    txtWriter.WriteLine(headerSeparator);
                }

                StringBuilder sbValues = new StringBuilder();
                foreach (KeyValuePair<string, object> value in row)
                {
                    string sValue = ValueFormatter.Format(value.Value);
                    string colValue = string.Format(colFormat, sValue);
                    if (_maxWidth < colValue.Length)
                    {
                        colValue = colValue.Substring(0, _maxWidth - 1);
                        colValue += "~";
                    }
                    sbValues.Append("| ").Append(colValue).Append(" ");
                }
                sbValues.Append(" |");
                string rowValues = sbValues.ToString();

                txtWriter.WriteLine(rowValues);
                txtWriter.WriteLine(rowSeparator);
            }
        }
    }
}