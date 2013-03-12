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

namespace CassandraSharp
{
    using CassandraSharp.Config;
    using CassandraSharp.Enlightenment;

    public static class ClusterManager
    {
        public static ICluster GetCluster(string name)
        {
            return EnglightenmentMgr.ClusterManager().GetCluster(name);
        }

        public static ICluster GetCluster(ClusterConfig clusterConfig)
        {
            return EnglightenmentMgr.ClusterManager().GetCluster(clusterConfig);
        }

        public static void Shutdown()
        {
            EnglightenmentMgr.ClusterManager().Shutdown();
        }

        public static void Configure(CassandraSharpConfig config)
        {
            EnglightenmentMgr.ClusterManager().Configure(config);
        }
    }
}