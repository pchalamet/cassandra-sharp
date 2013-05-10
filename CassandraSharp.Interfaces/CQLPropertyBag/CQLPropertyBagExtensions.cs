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
    using CassandraSharp.CQLCommand;
    using CassandraSharp.Enlightenment;

    public static class CQLPropertyBagExtensions
    {
        public static ICqlCommandBuilderTo FromPropertyBag(this ICqlCommandBuilderFrom @this)
        {
            var factory = EnglightenmentMgr.PropertyBagDataMapperFactory();
            return @this.Set(factory);
        }

        public static ICqlCommandBuilderBuild ToPropertyBag(this ICqlCommandBuilderTo @this)
        {
            var factory = EnglightenmentMgr.PropertyBagDataMapperFactory();
            return @this.Set(factory);
        }

        public static IPropertyBagCommand CreatePropertyBagCommand(this ICluster @this)
        {
            var cmd = @this.CreateCommand().FromPropertyBag().ToPropertyBag().Build();
            var pbCmd = new PropertyBagCommand(cmd);
            return pbCmd;
        }
    }
}