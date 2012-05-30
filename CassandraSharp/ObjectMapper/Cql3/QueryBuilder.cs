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

    public class QueryBuilder : IQueryBuilder
    {
        public string Build()
        {
            Validate();

            StringBuilder sb = new StringBuilder();
            string sep = "select ";
            foreach (string selector in Columns)
            {
                sb.AppendFormat("{0}{1}", sep, selector);
                sep = ",";
            }

            sb.AppendFormat(" from {0}", Table);

            if (null != ConsistencyLevel)
            {
                sb.AppendFormat(" using consistency {0}", ConsistencyLevel);
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

        public ConsistencyLevel? ConsistencyLevel { get; set; }

        public string Table { get; set; }

        public string[] Wheres { get; set; }

        public string[] Columns { get; set; }

        private void Validate()
        {
            Columns.CheckArrayHasAtLeastOneElement("Columns");
            Table.CheckArgumentNotNull("Table");
        }
    }
}