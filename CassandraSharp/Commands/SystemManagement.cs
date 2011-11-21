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
    using Apache.Cassandra;

    public static class SystemManagement
    {
        public static void SetKeySpace(Cassandra.Client client, string keyspace)
        {
            client.set_keyspace(keyspace);
        }

        public static string AddKeySpace(Cassandra.Client client, KsDef ksDef)
        {
            string schemaId = client.system_add_keyspace(ksDef);
            return schemaId;
        }

        public static string DropKeySpace(Cassandra.Client client, string keyspace)
        {
            string schemaId = client.system_drop_keyspace(keyspace);
            return schemaId;
        }

        public static string AddColumnFamily(Cassandra.Client client, CfDef cfDef)
        {
            string schemaId = client.system_add_column_family(cfDef);
            return schemaId;
        }

        public static string DropColumnFamily(Cassandra.Client client, string cfName)
        {
            string schemaId = client.system_drop_column_family(cfName);
            return schemaId;
        }

        public static void Login(Cassandra.Client client, string username, string password)
        {
            AuthenticationRequest authenticationRequest = new AuthenticationRequest();
            authenticationRequest.Credentials.Add("username", username);
            authenticationRequest.Credentials.Add("password", password);
            client.login(authenticationRequest);            
        }
    }
}