﻿// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2013 Pierre Chalamet
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

namespace CassandraSharp.Config
{
    using System.Xml;
    using System.Xml.Serialization;

    [XmlRoot("ClusterConfig")]
    public class ClusterConfig
    {
        public ClusterConfig()
        {
            Type = "Default";
            Partitioner = "Default";
        }

        [XmlElement("Endpoints")]
        public EndpointsConfig Endpoints { get; set; }

        [XmlAttribute("name")]
        public string Name { get; set; }

        [XmlAttribute("type")]
        public string Type { get; set; }

        [XmlAttribute("partitioner")]
        public string Partitioner { get; set; }

        [XmlElement("Transport")]
        public TransportConfig Transport { get; set; }

        [XmlElement("DefaultKeyspace")]
        public KeyspaceConfig DefaultKeyspace { get; set; }

        [XmlAnyAttribute]
        public XmlAttribute[] Extensions { get; set; }
    }
}