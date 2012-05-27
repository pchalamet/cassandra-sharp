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

namespace CassandraSharp.ObjectMapper.Cql3
{
    using System;
    using System.Text;
    using CassandraSharp.ObjectMapper.Dialect;

    public class CreateTableBuilder : ICreateTableBuilder
    {
        public string Build()
        {
            Validate();

            StringBuilder sb = new StringBuilder();

            // create table columns
            sb.AppendFormat("create table {0} (", Table);
            for (int i = 0; i < Columns.Length; ++i)
            {
                string name = Columns[i];
                string type = ColumnTypes[i].ToCql();
                sb.AppendFormat("{0} {1},", name, type);
            }

            string sep = "primary key (";
            foreach (string key in Keys)
            {
                sb.AppendFormat("{0}{1}", sep, key);
                sep = ",";
            }
            sb.Append("))");

            if (CompactStorage.HasValue && CompactStorage.Value)
            {
                sb.Append(" with compact storage");
            }

            return sb.ToString();
        }

        public string Table { get; set; }

        public string[] Columns { get; set; }

        public CqlType[] ColumnTypes { get; set; }

        public string[] Keys { get; set; }

        public bool? CompactStorage { get; set; }

        private void Validate()
        {
            if (null == Columns || 0 == Columns.Length)
            {
                throw new ArgumentException("Columns must have at least one element");
            }

            if (null == Table)
            {
                throw new ArgumentException("Table must be set");
            }

            if (null == ColumnTypes || 0 == ColumnTypes.Length)
            {
                throw new ArgumentException("ColumnTypes must have at least one element");
            }

            if (Columns.Length != ColumnTypes.Length)
            {
                throw new ArgumentException("Columns and ColumnTypes must have the same number of elements");
            }

            if (null == Keys || 0 == Keys.Length)
            {
                throw new ArgumentException("Keys must have at least one element");
            }
        }
    }
}