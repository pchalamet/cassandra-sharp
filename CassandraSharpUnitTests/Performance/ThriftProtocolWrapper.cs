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
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Apache.Cassandra;
    using CassandraSharp.Utils.Stream;
    using Thrift.Protocol;
    using Thrift.Transport;

    public class ThriftProtocolWrapper : BinaryProtocolWrapper
    {
        private Cassandra.Client _client;

        private CqlPreparedResult _query;

        public override string Name
        {
            get { return "Thrift"; }
        }

        public override void Open(string hostname)
        {
            base.Open(hostname);

            TTransport transport = new TFramedTransport(new TSocket(hostname, 9160));
            TProtocol protocol = new TBinaryProtocol(transport);
            _client = new Cassandra.Client(protocol);

            transport.Open();
        }

        public override void Dispose()
        {
            base.Dispose();

            _client.InputProtocol.Transport.Dispose();
        }

        private void TraceNextQuery()
        {
            byte[] traceIdBuffer = _client.trace_next_query();
            Guid traceId = traceIdBuffer.ToGuid();
            PerformanceInstrumentation.TracingIds.Add(traceId);            
        }

        public override void Query(string cmd)
        {
            TraceNextQuery();
            _client.execute_cql3_query(Encoding.UTF8.GetBytes(cmd), Compression.NONE, ConsistencyLevel.QUORUM);
        }

        public override void Prepare(string cmd)
        {
            TraceNextQuery();
            _query = _client.prepare_cql3_query(Encoding.UTF8.GetBytes(cmd), Compression.NONE);
        }

        public override void Execute(params object[] prms)
        {
            var listPrms = new List<byte[]>();
            foreach (object prm in prms)
            {
                if (prm is int)
                {
                    listPrms.Add(BitConverter.GetBytes((int) prm).Reverse().ToArray());
                }
                else
                {
                    listPrms.Add(Encoding.UTF8.GetBytes((string) prm));
                }
            }

            TraceNextQuery();
            _client.execute_prepared_cql3_query(_query.ItemId, listPrms, ConsistencyLevel.QUORUM);
        }
    }
}