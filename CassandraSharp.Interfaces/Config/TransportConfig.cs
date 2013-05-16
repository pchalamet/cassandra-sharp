// cassandra-sharp - high performance .NET driver for Apache Cassandra
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
    using System.Xml.Serialization;

    public class TransportConfig
    {
        [XmlAttribute("keepAlive")]
        public bool KeepAlive;

        [XmlAttribute("keepAliveTime")]
        public int KeepAliveTime;

        public TransportConfig()
        {
            Type = "Default";
            Port = 9042;
            Recoverable = true;
            CqlVersion = "3.0.0";
            DefaultConsistencyLevel = ConsistencyLevel.QUORUM;
            DefaultExecutionFlags = ExecutionFlags.None;
            KeepAlive = true;
        }

        [XmlAttribute("port")]
        public int Port { get; set; }

        [XmlAttribute("type")]
        public string Type { get; set; }

        [XmlAttribute("rcvTimeout")]
        public int ReceiveTimeout { get; set; }

        [XmlAttribute("sndTimeout")]
        public int SendTimeout { get; set; }

        [XmlAttribute("recoverable")]
        public bool Recoverable { get; set; }

        [XmlAttribute("user")]
        public string User { get; set; }

        [XmlAttribute("password")]
        public string Password { get; set; }

        [XmlAttribute("cqlver")]
        public string CqlVersion { get; set; }

        [XmlAttribute("cl")]
        public ConsistencyLevel DefaultConsistencyLevel { get; set; }

        [XmlAttribute]
        public ExecutionFlags DefaultExecutionFlags { get; set; }
    }
}