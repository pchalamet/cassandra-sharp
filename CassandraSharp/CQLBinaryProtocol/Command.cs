// cassandra-sharp - a .NET client for Apache Cassandra
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

namespace CassandraSharp.CQLBinaryProtocol
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reactive;
    using System.Reactive.Linq;
    using System.Threading.Tasks;
    using CassandraSharp.Extensibility;

    public abstract class Command : ICqlCommand
    {
        protected readonly ICluster Cluster;

        protected readonly IDataMapper DataMapper;

        protected Command(ICluster cluster, IDataMapper dataMapper)
        {
            Cluster = cluster;
            DataMapper = dataMapper;
        }

        public Task<IEnumerable<T>> Execute<T>(string cql, ConsistencyLevel cl = ConsistencyLevel.QUORUM,
                                               ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            IDataMapperFactory factory = DataMapper.Create<T>();
            IConnection connection = Cluster.GetConnection(null);
            IQuery query = CQLCommandHelpers.Query(connection, cql, cl, factory, executionFlags);

            IEnumerable<T> dataStream = null;
            Task<IEnumerable<T>> taskReader = new Task<IEnumerable<T>>(() => dataStream);

            Action<IEnumerable<object>> dataAvailable = ds =>
                {
                    dataStream = ds.Cast<T>();
                    taskReader.Start();
                };
            query.Schedule(dataAvailable);

            return taskReader;
        }

        public Task Execute(string cql, ConsistencyLevel cl = ConsistencyLevel.QUORUM, ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            IDataMapperFactory factory = DataMapper.Create<Unit>();
            IConnection connection = Cluster.GetConnection(null);
            IQuery query = CQLCommandHelpers.Query(connection, cql, cl, factory, executionFlags);

            Task taskReader = new Task(() => { });

            Action<IEnumerable<object>> dataAvailable = _ => taskReader.Start();
            query.Schedule(dataAvailable);

            return taskReader;
        }

        public IObservable<T> ExecuteQuery<T>(string cql, ConsistencyLevel cl = ConsistencyLevel.QUORUM, ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            IDataMapperFactory factory = DataMapper.Create<T>();
            IConnection connection = Cluster.GetConnection(null);
            IQuery query = CQLCommandHelpers.Query(connection, cql, cl, factory, executionFlags);

            return Observable.Create<T>(obs =>
                {
                    Action<IEnumerable<object>> dataAvailable = data =>
                        {
                            try
                            {
                                foreach (T datum in data)
                                {
                                    obs.OnNext(datum);
                                }
                                obs.OnCompleted();
                            }
                            catch (Exception ex)
                            {
                                obs.OnError(ex);
                            }
                        };

                    query.Schedule(dataAvailable);
                    return query;
                });
        }

        public IObservable<Unit> ExecuteNonQuery(string cql, ConsistencyLevel cl = ConsistencyLevel.QUORUM, ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            return ExecuteQuery<Unit>(cql, cl, executionFlags);
        }

        public IPreparedQuery<T> Prepare<T>(string cql, ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            return new PreparedQuery<T>(Cluster, DataMapper, cql, executionFlags);
        }

        public IPreparedQuery Prepare(string cql, ExecutionFlags executionFlags = ExecutionFlags.None)
        {
            return new PreparedNonQuery(Cluster, DataMapper, cql, executionFlags);
        }
    }
}