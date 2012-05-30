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

namespace CassandraSharpUnitTests.Utils
{
    using System.IO;
    using System.Xml;
    using System.Xml.Serialization;
    using CassandraSharp.Utils;
    using NUnit.Framework;

    [TestFixture]
    public class MiniXmlSerializerTest
    {
        [XmlRoot("RootClass")]
        public class TestRootClass
        {
            [XmlAttribute("nullableIntAttribute")]
            public int? NullableIntAttribute;

            [XmlElement("NullableIntElement")]
            public int? NullableIntElement;

            [XmlElement("StringElement")]
            public string StringElement;
        }

        [Test]
        public void TestDeserialize()
        {
            string xml =
                @"<RootClass nullableIntAttribute='42'>
                   <StringElement>666</StringElement>
                 </RootClass>";

            MiniXmlSerializer xmlSer = new MiniXmlSerializer(typeof(TestRootClass));

            TestRootClass rootClass;
            using (TextReader txtReader = new StringReader(xml))
            using (XmlReader xmlReader = XmlReader.Create(txtReader))
                rootClass = (TestRootClass) xmlSer.Deserialize(xmlReader);

            Assert.IsNotNull(rootClass);
            Assert.IsNotNull(rootClass.NullableIntAttribute);
            Assert.IsNull(rootClass.NullableIntElement);
            Assert.IsNotNull(rootClass.StringElement);
            Assert.AreEqual(42, rootClass.NullableIntAttribute);
            Assert.AreEqual("666", rootClass.StringElement);
        }

        [Test]
        public void TestMalformedXml()
        {
            string xml =
                @"<RootClass nullableIntAttribute='42'>
                   <StringElement>666</StringElemen>
                 </RootClass>";

            MiniXmlSerializer xmlSer = new MiniXmlSerializer(typeof(TestRootClass));

            using (TextReader txtReader = new StringReader(xml))
            using (XmlReader xmlReader = XmlReader.Create(txtReader))
            {
                Assert.Throws<XmlException>(() => xmlSer.Deserialize(xmlReader));
            }
        }

        [Test]
        public void TestMalformedXmlMissingEnd()
        {
            string xml =
                @"<RootClass nullableIntAttribute='42'>
                   <StringElement>666</StringElement>
                 ";

            MiniXmlSerializer xmlSer = new MiniXmlSerializer(typeof(TestRootClass));

            using (TextReader txtReader = new StringReader(xml))
            using (XmlReader xmlReader = XmlReader.Create(txtReader))
            {
                Assert.Throws<XmlException>(() => xmlSer.Deserialize(xmlReader));
            }
        }

    }
}