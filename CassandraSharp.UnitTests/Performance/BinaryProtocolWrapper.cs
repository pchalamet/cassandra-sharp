// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2018 Pierre Chalamet
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

using System;
using CassandraSharp.Config;
using CassandraSharp.CQLOrdinal;
using CassandraSharp.Extensibility;

namespace CassandraSharp.UnitTests.Performance
{
    public class BinaryProtocolWrapper : ProtocolWrapper
    {
        private ICluster _cluster;
        private IClusterManager _clusterManager;

        private ICqlCommand _cmd;

        private IPreparedQuery<NonQuery> _prepared;

        public override string Name => "BinaryProtocol";

        public override void Open(string hostname)
        {
            //run Write Performance Test using cassandra-sharp driver
            var cassandraSharpConfig = new CassandraSharpConfig();
            cassandraSharpConfig.Instrumentation = new InstrumentationConfig();
            cassandraSharpConfig.Instrumentation.Type = typeof(PerformanceInstrumentation).AssemblyQualifiedName;
            _clusterManager = new ClusterManager(cassandraSharpConfig);

            var clusterConfig = new ClusterConfig
                                {
                                    Endpoints = new EndpointsConfig
                                                {
                                                    Servers = new[] {hostname}
                                                }
                                };

            _cluster = _clusterManager.GetCluster(clusterConfig);

            _cmd = _cluster.CreateOrdinalCommand();
        }

        public override void Dispose()
        {
            if (null != _prepared) _prepared.Dispose();

            _cluster.Dispose();
            _clusterManager.Dispose();
        }

        public override void Query(string cmd)
        {
            _cmd.WithExecutionFlags(ExecutionFlags.ServerTracing).Execute(cmd).AsFuture().Wait();
        }

        public override void Prepare(string cmd)
        {
            _prepared = _cmd.WithExecutionFlags(ExecutionFlags.ServerTracing).Prepare(cmd);
        }

        public override void Execute(params object[] prms)
        {
            _prepared.Execute(prms).AsFuture().Wait();
        }

        public override TracingSession QueryTracingInfo(Guid tracingId)
        {
            return _cluster.QueryTracingInfo(tracingId);
        }
    }
}