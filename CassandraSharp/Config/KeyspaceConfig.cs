// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2018 Pierre Chalamet
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

using System.Xml.Serialization;

namespace CassandraSharp.Config
{
    [XmlRoot("DefaultKeyspace")]
    public class KeyspaceConfig
    {
        public KeyspaceConfig()
        {
            DurableWrites = true;
            Replication = Replication ?? new ReplicationConfig();
        }

        [XmlAttribute("name")]
        public string Name { get; set; }

        [XmlAttribute("durableWrites")]
        public bool DurableWrites { get; set; }

        [XmlElement("Replication")]
        public ReplicationConfig Replication { get; set; }
    }
}