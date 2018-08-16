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
    using System;
    using System.Collections.Generic;
    using System.Xml;
    using System.Xml.Schema;
    using System.Xml.Serialization;

    [XmlRoot("Replication")]
    public class ReplicationConfig : IXmlSerializable
    {
        public Dictionary<string, string> Options { get; set; }

        public ReplicationConfig()
        {
            Options = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
            Options["class"] = "SimpleStrategy";
            Options["replication_factor"] = "1";
        }

        public XmlSchema GetSchema()
        {
            return null;
        }

        public void ReadXml(XmlReader reader)
        {
            bool wasEmpty = reader.IsEmptyElement;

            Options = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
            for (int i = 0; i < reader.AttributeCount; i++)
            {
                reader.MoveToAttribute(i);
                Options[reader.Name] = reader.Value;
            }

            reader.Read();

            if (!wasEmpty)
            {
                reader.Skip();
            }
        }

        public void WriteXml(XmlWriter writer)
        {
            throw new NotImplementedException();
        }
    }
}