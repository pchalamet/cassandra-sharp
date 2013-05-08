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

namespace CassandraSharp.CQLPoco
{
    using CassandraSharp.CQLCommand;
    using CassandraSharp.Enlightenment;

    public static class CQLPocoExtensions
    {
        public static ICqlCommandBuilderTo FromPoco(this ICqlCommandBuilderFrom @this)
        {
            var factory = EnglightenmentMgr.PocoDataMapperFactory();
            return @this.SetFactory(factory);
        }

        public static ICqlCommandBuilderBuild ToPoco(this ICqlCommandBuilderTo @this)
        {
            var factory = EnglightenmentMgr.PocoDataMapperFactory();
            return @this.SetFactory(factory);
        }

        public static ICqlCommand CreatePocoCommand(this ICluster @this)
        {
            return @this.CreateCommand().FromPoco().ToPoco().Build();
        }
    }
}