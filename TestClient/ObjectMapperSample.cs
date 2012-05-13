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

namespace TestClient
{
    using CassandraSharp;
    using CassandraSharp.ObjectMapper;

    [Schema("TestKeyspace", Comment = "People table")]
    public class PeopleSchema
    {
        [Index(Name = "birthyear")]
        public int Birthyear;

        [Key(Name = "firstname")]
        public string FirstName;

        [Column(Name = "lastname")]
        public string LastName;
    }

    public class ObjectMapperSample
    {
        private readonly string _configName;

        public ObjectMapperSample(string configName)
        {
            _configName = configName;
        }

        public void Run()
        {
            using (ICluster cluster = ClusterManager.GetCluster(_configName))
                Run(cluster);
        }

        protected void Run(ICluster cluster)
        {
            cluster.Drop<PeopleSchema>();
            cluster.Create<PeopleSchema>();

            cluster.Execute("insert into People (firstname, lastname, birthyear) values (?, ?, ?)",
                            new {firstname = "isabelle", lastname = "chalamet", birthyear = 1972},
                            new {firstname = "pierre", lastname = "chalamet", birthyear = 1973});
        }
    }
}