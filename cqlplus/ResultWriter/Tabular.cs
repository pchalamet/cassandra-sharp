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

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using CassandraSharp.CQLPropertyBag;

namespace cqlplus.ResultWriter
{
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
                    var headerSeparator = rowSeparator.Replace('-', '=');
                    var header = BuildRowValues(row, colWidths, (key, value) => key);

                    txtWriter.WriteLine(rowSeparator);
                    txtWriter.WriteLine(header);
                    txtWriter.WriteLine(headerSeparator);
                }

                var rowValues = BuildRowValues(row, colWidths, (key, value) => ValueFormatter.Format(value));
                txtWriter.WriteLine(rowValues);
                txtWriter.WriteLine(rowSeparator);
            }
        }

        private static Dictionary<string, int> DetermineColumnWidth(PropertyBag row, int maxWidth)
        {
            var colWidths = new Dictionary<string, int>();
            foreach (var col in row.Keys)
            {
                var sValue = ValueFormatter.Format(row[col]);
                var keyWidth = col.Length;
                var valWidth = sValue.Length;
                var colWidth = 4 + Math.Max(keyWidth, valWidth);
                colWidth = Math.Min(colWidth, maxWidth);
                colWidths.Add(col, colWidth);
            }

            return colWidths;
        }

        private static string BuildRowSeparator(Dictionary<string, int> colWidths)
        {
            var sbRowSeparator = new StringBuilder();
            foreach (var kvp in colWidths)
            {
                var colWidth = colWidths[kvp.Key];
                sbRowSeparator.Append("+-").Append(new string('-', colWidth)).Append('-');
            }

            sbRowSeparator.Append("-+");

            var rowSeparator = sbRowSeparator.ToString();
            return rowSeparator;
        }

        private static string BuildRowValues(PropertyBag row, IDictionary<string, int> colWidths,
                                             Func<string, object, string> formatter)
        {
            var sbValues = new StringBuilder();
            foreach (var col in row.Keys)
            {
                var sValue = formatter(col, row[col]);
                var colWidth = colWidths[col];
                var colFormat = string.Format("{{0,-{0}}}", colWidth);

                var colValue = string.Format(colFormat, sValue);
                if (colWidth < colValue.Length)
                {
                    colValue = colValue.Substring(0, colWidth - 1);
                    colValue += "~";
                }

                sbValues.Append("| ").Append(colValue).Append(" ");
            }

            sbValues.Append(" |");
            var rowValues = sbValues.ToString();
            return rowValues;
        }
    }
}