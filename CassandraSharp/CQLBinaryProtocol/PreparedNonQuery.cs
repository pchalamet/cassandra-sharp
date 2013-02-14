//// cassandra-sharp - a .NET client for Apache Cassandra
//// Copyright (c) 2011-2013 Pierre Chalamet
//// 
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
//// 
//// http://www.apache.org/licenses/LICENSE-2.0
//// 
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.

//namespace CassandraSharp.CQLBinaryProtocol
//{
//    using System;
//    using System.Linq;
//    using System.Reactive;
//    using System.Threading.Tasks;
//    using CassandraSharp.CQL;
//    using CassandraSharp.Extensibility;

//    internal class PreparedNonQuery : IPreparedQuery
//    {
//        private readonly IDataMapper _dataMapper;

//        private readonly CQLPreparedQueryHelpers _helpers;

//        public PreparedNonQuery(ICluster cluster, IDataMapper dataMapper, string cql, ExecutionFlags executionFlags)
//        {
//            _dataMapper = dataMapper;
//            _helpers = new CQLPreparedQueryHelpers(cluster, cql, executionFlags);
//        }

//        public Task<int> Execute(object dataSource, ConsistencyLevel cl)
//        {
//            IDataMapperFactory factory = _dataMapper.Create<Unit>(dataSource);
//            return _helpers.Execute(factory, cl).ContinueWith(res => res.Result.Count());
//        }

//        public IObservable<Unit> ExecuteQuery(object dataSource, ConsistencyLevel cl = ConsistencyLevel.QUORUM)
//        {
//            IDataMapperFactory factory = _dataMapper.Create<Unit>(dataSource);
//        }
//    }
//}