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

    public class UpdateBuilder : IUpdateBuilder
    {
        public string Table { get; set; }

        public string Build()
        {
            Validate();

            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("update {0}", Table);

            string sep = " using ";
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

            sep = " set ";
            for (int i = 0; i < Columns.Length; ++i)
            {
                sb.AppendFormat("{0}{1}={2}", sep, Columns[i], Values[i]);
                sep = ",";
            }

            if (null != Wheres)
            {
                sep = " where ";
                foreach (string where in Wheres)
                {
                    sb.AppendFormat("{0}{1}", sep, where);
                    sep = " and ";
                }
            }

            return sb.ToString();
        }

        public string[] Columns { get; set; }

        public string[] Values { get; set; }

        public long? Timestamp { get; set; }

        public long? TTL { get; set; }

        public string[] Wheres { get; set; }

        public ConsistencyLevel? ConsistencyLevel { get; set; }

        private void Validate()
        {
            Table.CheckArgumentNotNull("Table");
            Columns.CheckArrayHasAtLeastOneElement("Columns");
            Values.CheckArrayHasAtLeastOneElement("Values");
            Columns.CheckArrayIsSameLengthAs(Values, "Columns", "Values");
        }
    }
}