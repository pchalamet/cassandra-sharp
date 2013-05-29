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

namespace CassandraSharpUnitTests.Performance
{
    using System;
    using CassandraSharp;
    using CassandraSharp.CQLOrdinal;
    using CassandraSharp.Config;
    using CassandraSharp.Extensibility;

    public class BinaryProtocolWrapper : ProtocolWrapper
    {
        private ICluster _cluster;

        private ICqlCommand _cmd;

        private IPreparedQuery<NonQuery> _prepared;

        public override string Name
        {
            get { return "BinaryProtocol"; }
        }

        public override void Open(string hostname)
        {
            //run Write Performance Test using cassandra-sharp driver
            CassandraSharpConfig cassandraSharpConfig = new CassandraSharpConfig();
            cassandraSharpConfig.Instrumentation = new InstrumentationConfig();
            cassandraSharpConfig.Instrumentation.Type = typeof(PerformanceInstrumentation).AssemblyQualifiedName;
            ClusterManager.Configure(cassandraSharpConfig);

            ClusterConfig clusterConfig = new ClusterConfig
                {
                        Endpoints = new EndpointsConfig
                            {
                                    Servers = new[] {
                                        new ServerConfig() {
                                            Server = hostname
                                        }
                                    }
                            },
                };

            _cluster = ClusterManager.GetCluster(clusterConfig);

            _cmd = _cluster.CreateOrdinalCommand();
        }

        public override void Dispose()
        {
            _cluster.Dispose();
            ClusterManager.Shutdown();
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