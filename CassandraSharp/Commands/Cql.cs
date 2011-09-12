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
    using System.Text;
    using Apache.Cassandra;

    internal static class Cql
    {
        public static CqlResult Command(Cassandra.Client client, string command)
        {
            byte[] query = Encoding.UTF8.GetBytes(command);

            return client.execute_cql_query(query, Compression.NONE);
        }
    }
}