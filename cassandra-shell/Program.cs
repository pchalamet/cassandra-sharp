/* 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
namespace cassandra_shell
{
    using System;
    using System.IO;
    using System.Text;
    using System.Xml;
    using System.Xml.Serialization;
    using Apache.Cassandra;
    using CassandraSharp;
    using CassandraSharp.Config;

    public static class Program
    {
        public static void Main(string[] args)
        {
            string server = args[0];
            string port = args[1];

            Init(server, port);

            ICluster cluster = ClusterManager.GetCluster("Shell");

            while (true)
            {
                StringBuilder sb = new StringBuilder();
                Console.Write(">");
                string line = Console.ReadLine();
                sb.Append(line).Append(' ');
                while (! line.Contains(";"))
                {
                    Console.Write("...");
                    line = Console.ReadLine();
                    sb.Append(line).Append(' ');
                }

                line = sb.ToString().Trim();
                if (line.StartsWith("!"))
                {
                    ExecuteMetaCommand(cluster, line);
                }
                else
                {
                    ExecuteCqlCommand(cluster, line);
                }
            }
        }

        private static void ExecuteMetaCommand(ICluster cluster, string line)
        {
        }

        private static void ExecuteCqlCommand(ICluster cluster, string line)
        {
            try
            {
                CqlResult result = cluster.ExecuteCql(line);
                Console.WriteLine(result.Type);
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }
        }

        private static void Init(string server, string port)
        {
            const string template =
                @"<CassandraSharpConfig>
        <Cluster name='Shell'>
            <Behavior maxRetries='3'
                      strategy='NetworkTopology' />

            <Transport type='Framed'
                       port='{1}' />

            <Endpoints snitch='RackInferring' strategy='Random'>
                <Server>{0}</Server>
            </Endpoints>
        </Cluster>

    </CassandraSharpConfig>";

            string xmlConfig = string.Format(template, server, port);
            CassandraSharpConfig config;
            XmlSerializer xmlSerializer = new XmlSerializer(typeof(CassandraSharpConfig));
            using (TextReader txtReader = new StringReader(xmlConfig))
            using (XmlReader xmlReader = XmlReader.Create(txtReader))
                config = (CassandraSharpConfig) xmlSerializer.Deserialize(xmlReader);

            ClusterManager.Configure(config);
        }
    }
}