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
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using CassandraSharp.Config;

    public static class ClusterManager
    {
        private static readonly Lazy<IClusterManager> _instance = new Lazy<IClusterManager>(CreateClusterManager);

        public static Task<IList<T>> AsFuture<T>(this IObservable<T> observable)
        {
            return _instance.Value.AsFuture(observable);
        }

        public static Task AsFuture(this IObservable<NonQuery> observable)
        {
            return _instance.Value.AsFuture(observable);
        }

        private static IClusterManager CreateClusterManager()
        {
            const string typeName = "CassandraSharp.Cluster.DefaultClusterManager, CassandraSharp";
            Type type = Type.GetType(typeName, true);
            IClusterManager clusterManager = (IClusterManager) Activator.CreateInstance(type);
            return clusterManager;
        }

        public static ICluster GetCluster(string name)
        {
            return _instance.Value.GetCluster(name);
        }

        public static ICluster GetCluster(ClusterConfig clusterConfig)
        {
            return _instance.Value.GetCluster(clusterConfig);
        }

        public static void Shutdown()
        {
            _instance.Value.Shutdown();
        }

        public static void Configure(CassandraSharpConfig config)
        {
            _instance.Value.Configure(config);
        }
    }
}