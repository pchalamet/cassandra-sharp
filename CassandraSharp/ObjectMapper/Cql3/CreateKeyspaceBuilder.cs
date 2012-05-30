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
    using System.Collections.Generic;
    using System.Text;
    using CassandraSharp.ObjectMapper.Dialect;
    using CassandraSharp.Utils;

    internal class CreateKeyspaceBuilder : ICreateKeyspaceBuilder
    {
        public string Build()
        {
            Validate();

            StringBuilder sb = new StringBuilder();

            sb.AppendFormat("create keyspace {0}", Keyspace);

            string strategyClass = StrategyClass ?? "SimpleStrategy";
            sb.AppendFormat(" with strategy_class={0}", strategyClass);

            Dictionary<string, int> replicationFactor = ReplicationFactor ?? new Dictionary<string, int>
                                                                                 {
                                                                                     {"replication_factor", 1}
                                                                                 };
            foreach(var stratOpt in replicationFactor)
            {
                sb.AppendFormat(" and strategy_options:{0}={1}", stratOpt.Key, stratOpt.Value);
            }

            return sb.ToString();
        }

        public string Keyspace { get; set; }

        public string StrategyClass { get; set; }

        public Dictionary<string, int> ReplicationFactor { get; set; }

        private void Validate()
        {
            Keyspace.CheckArgumentNotNull("Keyspace");
        }
    }
}