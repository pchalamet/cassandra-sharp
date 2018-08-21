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

using CassandraSharp.Core.CQLBinaryProtocol;
using CassandraSharp.Core.Utils;

namespace CassandraSharp.CQLCommand
{
    using CassandraSharp.Extensibility;

    internal sealed class CqlCommandBuilder : ICqlCommandBuilderFrom,
                                              ICqlCommandBuilderTo,
                                              ICqlCommandBuilderBuild
    {
        private readonly ICluster _cluster;

        private IDataMapperFactory _factoryFrom;

        private IDataMapperFactory _factoryTo;

        internal CqlCommandBuilder(ICluster cluster)
        {
            _cluster = cluster;
        }

        public ICqlCommand Build()
        {
            return new CqlCommand(_cluster, _factoryFrom, _factoryTo);
        }

        ICqlCommandBuilderTo ICqlCommandBuilderFrom.Set(IDataMapperFactory factory)
        {
            factory.CheckArgumentNotNull("factory");
            _factoryFrom = factory;
            return this;
        }

        ICqlCommandBuilderBuild ICqlCommandBuilderTo.Set(IDataMapperFactory factory)
        {
            factory.CheckArgumentNotNull("factory");
            _factoryTo = factory;
            return this;
        }
    }
}