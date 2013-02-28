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

namespace CassandraSharp.CQLPropertyBag
{
    using System;
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Extensibility;

    public class PropertyBagCommand : Command
    {
        internal PropertyBagCommand(ICluster cluster)
                : base(cluster, new PropertyBagMapperFactory())
        {
        }

        public IObservable<PropertyBag> Execute(string cql, ConsistencyLevel cl = ConsistencyLevel.QUORUM,
                                                ExecutionFlags executionFlags = ExecutionFlags.None,
                                                object key = null)
        {
            return Execute<PropertyBag>(cql, cl, executionFlags, key);
        }

        public IPreparedQuery<PropertyBag> Prepare(string cql, ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            return Prepare<PropertyBag>(cql, executionFlags);
        }

        private class PropertyBagMapperFactory : IDataMapper
        {
            public IDataMapperFactory Create<T>(object dataSource)
            {
                PropertyBag mapDataSource = (PropertyBag) dataSource;
                return new DataMapperFactory(mapDataSource);
            }
        }
    }
}