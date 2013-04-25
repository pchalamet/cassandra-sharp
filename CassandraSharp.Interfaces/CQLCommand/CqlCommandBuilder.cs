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

namespace CassandraSharp.CQLCommand
{
    using CassandraSharp.Enlightenment;
    using CassandraSharp.Extensibility;

    internal class CqlCommandBuilder : ICqlCommandBuilderFrom,
                                       ICqlCommandBuilderTo,
                                       ICqlCommandBuilderBuild
    {
        private readonly ICluster _cluster;

        private IDataMapperFactory _factoryIn;

        private IDataMapperFactory _factoryOut;

        internal CqlCommandBuilder(ICluster cluster)
        {
            _cluster = cluster;
            _factoryIn = null;
            _factoryOut = null;
        }

        public ICqlCommand Build()
        {
            return EnglightenmentMgr.CommandFactory().Create(_cluster, _factoryIn, _factoryOut);
        }

        public ICqlCommandBuilderTo FromOrdinal()
        {
            _factoryIn = EnglightenmentMgr.OrdinalDataMapperFactory();
            return this;
        }

        public ICqlCommandBuilderTo FromPoco()
        {
            _factoryIn = EnglightenmentMgr.PocoDataMapperFactory();
            return this;
        }

        public ICqlCommandBuilderTo FromPropertyBag()
        {
            _factoryIn = EnglightenmentMgr.PropertyBagDataMapperFactory();
            return this;
        }

        public ICqlCommandBuilderBuild ToOrdinal()
        {
            _factoryOut = EnglightenmentMgr.OrdinalDataMapperFactory();
            return this;
        }

        public ICqlCommandBuilderBuild ToPoco()
        {
            _factoryOut = EnglightenmentMgr.PocoDataMapperFactory();
            return this;
        }

        public ICqlCommandBuilderBuild ToPropertyBag()
        {
            _factoryOut = EnglightenmentMgr.PropertyBagDataMapperFactory();
            return this;
        }
    }
}