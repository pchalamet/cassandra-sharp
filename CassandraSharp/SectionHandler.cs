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

namespace CassandraSharp
{
    using System.Configuration;
    using System.Xml;
    using System.Xml.Serialization;
    using CassandraSharp.Config;

    public class SectionHandler : IConfigurationSectionHandler
    {
        public object Create(object parent, object configContext, XmlNode section)
        {
            XmlDocument xmlDoc = new XmlDocument();
            XmlNode xmlRootNode = xmlDoc.AppendChild(xmlDoc.CreateElement("CassandraSharpConfig"));
            xmlRootNode.InnerXml = section.InnerXml;

            return ReadConfig(xmlDoc);
        }

        private static CassandraSharpConfig ReadConfig(XmlDocument xmlDoc)
        {
            XmlSerializer xmlSer = new XmlSerializer(typeof(CassandraSharpConfig));
            using (XmlReader xmlReader = new XmlNodeReader(xmlDoc))
                return (CassandraSharpConfig) xmlSer.Deserialize(xmlReader);
        }
    }
}