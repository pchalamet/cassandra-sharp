﻿// cassandra-sharp - a .NET client for Apache Cassandra
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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using CassandraSharp.CQL;
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Extensibility;

    public static class CQLPocoExtensions
    {
        private static IDataMapperFactory GetFactory(Type type, object dataSource)
        {
            //IDataMapperFactory factory = new DataMapperFactory(type, dataSource);
            IDataMapperFactory factory = new DynamicDataMapperFactory(type, dataSource);
            return factory;
        }

        private static IDataMapperFactory GetFactory<T>(T dataSource)
        {
            //IDataMapperFactory factory = new DataMapperFactory(type, dataSource);
            IDataMapperFactory factory = new DynamicDataMapperFactory<T>(dataSource);
            return factory;
        }

        public static Task<IEnumerable<T>> Execute<T>(this ICluster cluster, string cql, ConsistencyLevel cl = ConsistencyLevel.QUORUM)
        {
            IDataMapperFactory factory = GetFactory<T>(default(T));
            return CQLCommandHelpers.Query<T>(cluster, cql, cl, factory);
        }

        public static Task<IEnumerable<T>> Execute<T>(this IPreparedQuery preparedQuery, T dataSource, ConsistencyLevel cl = ConsistencyLevel.QUORUM)
        {
            IDataMapperFactory factory = GetFactory<T>(dataSource);
            return preparedQuery.Execute(cl, factory).ContinueWith(res => res.Result.Cast<T>());
        }

        public static Task<int> ExecuteNonQuery(this IPreparedQuery preparedQuery, object dataSource, ConsistencyLevel cl = ConsistencyLevel.QUORUM)
        {
            IDataMapperFactory factory = GetFactory(typeof(Unit), dataSource);
            return preparedQuery.Execute(cl, factory).ContinueWith(res => res.Result.Count());
        }
        
        public static Task<int> ExecuteNonQuery<T>(this IPreparedQuery preparedQuery, T dataSource, ConsistencyLevel cl = ConsistencyLevel.QUORUM)
        {
            IDataMapperFactory factory = GetFactory<T>(dataSource);
            return preparedQuery.Execute(cl, factory).ContinueWith(res => res.Result.Count());
        }
    }
}