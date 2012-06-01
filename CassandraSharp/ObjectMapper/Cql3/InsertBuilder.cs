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

namespace CassandraSharp.ObjectMapper.Cql3
{
    using System.Text;
    using Apache.Cassandra;
    using CassandraSharp.ObjectMapper.Dialect;
    using CassandraSharp.Utils;

    public class InsertBuilder : IInsertBuilder
    {
        public string Build()
        {
            Validate();

            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("insert into {0} (", Table);
            string sep = "";
            foreach (string selector in Columns)
            {
                sb.AppendFormat("{0}{1}", sep, selector);
                sep = ",";
            }

            sep = ") values (";
            foreach (string value in Values)
            {
                sb.AppendFormat("{0}{1}", sep, value);
                sep = ",";
            }
            sb.Append(")");

            sep = " using ";
            if (null != ConsistencyLevel)
            {
                sb.AppendFormat("{0}consistency {1}", sep, ConsistencyLevel.Value);
                sep = " and ";
            }

            if (null != Timestamp)
            {
                sb.AppendFormat("{0}timestamp {1}", sep, Timestamp.Value);
                sep = " and ";
            }

            if (null != TTL)
            {
                sb.AppendFormat("{0}ttl {1}", sep, TTL.Value);
            }

            return sb.ToString();
        }

        public ConsistencyLevel? ConsistencyLevel { get; set; }

        public string Table { get; set; }

        public string[] Columns { get; set; }

        public string[] Values { get; set; }

        public long? Timestamp { get; set; }

        public long? TTL { get; set; }

        private void Validate()
        {
            Table.CheckArgumentNotNull("Table");
            Columns.CheckArrayHasAtLeastOneElement("Columns");
            Values.CheckArrayHasAtLeastOneElement("Values");
            Columns.CheckArrayIsSameLengthAs(Values, "Columns", "Values");
        }
    }
}