// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
// limitations under the License.
namespace CassandraSharp.Commands
{
    using System.Collections.Generic;
    using Apache.Cassandra;

    public class Describe
    {
        public static string ClusterName(Cassandra.Client client)
        {
            return client.describe_cluster_name();
        }

        public static KsDef Keyspace(Cassandra.Client client, string keyspace)
        {
            return client.describe_keyspace(keyspace);
        }

        public static IEnumerable<KsDef> KeySpaces(Cassandra.Client client)
        {
            return client.describe_keyspaces();
        }

        public static string Partitioner(Cassandra.Client client)
        {
            return client.describe_partitioner();
        }

        public static IEnumerable<TokenRange> Ring(Cassandra.Client client, string keyspace)
        {
            return client.describe_ring(keyspace);
        }

        public static string Snitch(Cassandra.Client client)
        {
            return client.describe_snitch();
        }

        public static string Version(Cassandra.Client client)
        {
            return client.describe_version();
        }
    }
}