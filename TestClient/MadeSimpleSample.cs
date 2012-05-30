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

namespace TestClient
{
    using CassandraSharp;
    using CassandraSharp.MadeSimple;

    public class MadeSimpleSample : Sample
    {
        public MadeSimpleSample(string config)
            : base("MadeSimple", config)
        {
        }

        protected override void CreateSchema(ICluster cluster)
        {
            cluster.CreateTable("Users");
        }

        protected override void DropSchema(ICluster cluster)
        {
            cluster.DropTable("Users");
        }

        protected override void RunSample(ICluster cluster)
        {
            cluster.Insert("Users", new Utf8NameOrValue("User1"), new Utf8NameOrValue("Name"), new Utf8NameOrValue("RealName1"));
            cluster.Insert("Users", new Utf8NameOrValue("User1"), new Utf8NameOrValue("Location"), new Utf8NameOrValue("SF"));

            cluster.Insert("Users", new Utf8NameOrValue("User2"), new Utf8NameOrValue("Name"), new Utf8NameOrValue("RealName2"));
            cluster.Insert("Users", new Utf8NameOrValue("User2"), new Utf8NameOrValue("Location"), new Utf8NameOrValue("NY"));

            cluster.Insert("Users", new Utf8NameOrValue("User3"), new Utf8NameOrValue("Name"), new Utf8NameOrValue("RealName3"));
            cluster.Insert("Users", new Utf8NameOrValue("User3"), new Utf8NameOrValue("Location"), new Utf8NameOrValue("SF"));

            cluster.Insert("Users", new Utf8NameOrValue("User4"), new Utf8NameOrValue("Name"), new Utf8NameOrValue("RealName4"));
            cluster.Insert("Users", new Utf8NameOrValue("User4"), new Utf8NameOrValue("Location"), new Utf8NameOrValue("HK"));
        }
    }
}