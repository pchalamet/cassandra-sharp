// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
// limitations under the License.
namespace TestClient
{
    using System;
    using CassandraSharp;
    using CassandraSharp.Config;
    using CassandraSharp.Model;

    internal class Program
    {
        private static void Main(string[] args)
        {
            XmlConfigurator.Configure();

            // using declarative configuration
            using (ICluster cluster = ClusterManager.GetCluster("TestCassandra"))
                TestCluster(cluster);

            // using minimal declarative configuration
            // no keyspace is specified : we need to set ICommandInfo before calling the command
            using (ICluster cluster = ClusterManager.GetCluster("TestCassandraMinimal"))
            {
                BehaviorConfigBuilder cmdInfoBuilder = new BehaviorConfigBuilder { KeySpace = "TestKS" };
                ICluster configuredCluster = cluster.Configure(cmdInfoBuilder);
                TestCluster(configuredCluster);
            }

            ClusterManager.Shutdown();
        }

        private static void TestCluster(ICluster cluster)
        {
            string clusterName = cluster.DescribeClusterName();
            Console.WriteLine(clusterName);

            cluster.Truncate("CF");

            var nvColumn = new Utf8NameOrValue("column");
            var nvKey = new Utf8NameOrValue("key");
            var nvValue = new ByteArrayNameOrValue(new byte[10]);
            cluster.Insert(columnFamily: "CF",
                           key: nvKey,
                           column: nvColumn,
                           value: nvValue);
        }
    }
}