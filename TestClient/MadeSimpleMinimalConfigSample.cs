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

namespace TestClient
{
    using CassandraSharp;
    using CassandraSharp.Config;

    public class MadeSimpleMinimalConfigSample : MadeSimpleSample
    {
        public MadeSimpleMinimalConfigSample()
            : base("MinimalConfig")
        {
        }

        protected override void CreateSchema(ICluster cluster)
        {
            IBehaviorConfig cfgBuilder = new BehaviorConfig {KeySpace = "MadeSimple"};
            using (ICluster configuredCluster = cluster.CreateChildCluster(cfgBuilder))
                base.CreateSchema(configuredCluster);
        }

        protected override void DropSchema(ICluster cluster)
        {
            IBehaviorConfig cfgBuilder = new BehaviorConfig {KeySpace = "MadeSimple"};
            using (ICluster configuredCluster = cluster.CreateChildCluster(cfgBuilder))
                base.DropSchema(configuredCluster);
        }

        protected override void RunSample(ICluster cluster)
        {
            IBehaviorConfig cfgBuilder = new BehaviorConfig {KeySpace = "MadeSimple"};
            using (ICluster configuredCluster = cluster.CreateChildCluster(cfgBuilder))
                base.RunSample(configuredCluster);
        }
    }
}