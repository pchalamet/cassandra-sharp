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

namespace cqlplus.ResultWriter
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using CassandraSharp.CQLPropertyBag;

    public class Tabular : IResultWriter
    {
        private readonly int _maxWidth;

        public Tabular(int maxWidth)
        {
            _maxWidth = maxWidth;
        }

        public void Write(TextWriter txtWriter, IEnumerable<PropertyBag> rowSet)
        {
            string rowSeparator = null;

            Dictionary<string, int> colWidths = null;
            foreach (var row in rowSet)
            {
                if (null == colWidths)
                {
                    colWidths = DetermineColumnWidth(row, _maxWidth);

                    rowSeparator = BuildRowSeparator(colWidths);
                    string headerSeparator = rowSeparator.Replace('-', '=');
                    string header = BuildRowValues(row, colWidths, (key, value) => key);

                    txtWriter.WriteLine(rowSeparator);
                    txtWriter.WriteLine(header);
                    txtWriter.WriteLine(headerSeparator);
                }

                string rowValues = BuildRowValues(row, colWidths, (key, value) => ValueFormatter.Format(value));
                txtWriter.WriteLine(rowValues);
                txtWriter.WriteLine(rowSeparator);
            }
        }

        private static Dictionary<string, int> DetermineColumnWidth(PropertyBag row, int maxWidth)
        {
            Dictionary<string, int> colWidths = new Dictionary<string, int>();
            foreach (var col in row.Keys)
            {
                string sValue = ValueFormatter.Format(row[col]);
                int keyWidth = col.Length;
                int valWidth = sValue.Length;
                int colWidth = 4 + Math.Max(keyWidth, valWidth);
                colWidth = Math.Min(colWidth, maxWidth);
                colWidths.Add(col, colWidth);
            }
            return colWidths;
        }

        private static string BuildRowSeparator(Dictionary<string, int> colWidths)
        {
            StringBuilder sbRowSeparator = new StringBuilder();
            foreach (var kvp in colWidths)
            {
                int colWidth = colWidths[kvp.Key];
                sbRowSeparator.Append("+-").Append(new string('-', colWidth)).Append('-');
            }
            sbRowSeparator.Append("-+");

            string rowSeparator = sbRowSeparator.ToString();
            return rowSeparator;
        }

        private static string BuildRowValues(PropertyBag row, IDictionary<string, int> colWidths,
                                             Func<string, object, string> formatter)
        {
            StringBuilder sbValues = new StringBuilder();
            foreach (var col in row.Keys)
            {
                string sValue = formatter(col, row[col]);
                int colWidth = colWidths[col];
                string colFormat = string.Format("{{0,-{0}}}", colWidth);

                string colValue = string.Format(colFormat, sValue);
                if (colWidth < colValue.Length)
                {
                    colValue = colValue.Substring(0, colWidth - 1);
                    colValue += "~";
                }
                sbValues.Append("| ").Append(colValue).Append(" ");
            }
            sbValues.Append(" |");
            string rowValues = sbValues.ToString();
            return rowValues;
        }
    }
}