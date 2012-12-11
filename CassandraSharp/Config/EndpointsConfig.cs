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

namespace CassandraSharp.Config
{
    using System.Xml.Serialization;

    public class EndpointsConfig
    {
        public EndpointsConfig()
        {
            Snitch = "RackInferring";
            Strategy = "Nearest";
        }

        [XmlElement("Server")]
        public string[] Servers { get; set; }

        [XmlAttribute("snitch")]
        public string Snitch { get; set; }

        [XmlAttribute("strategy")]
        public string Strategy { get; set; }
    }
}