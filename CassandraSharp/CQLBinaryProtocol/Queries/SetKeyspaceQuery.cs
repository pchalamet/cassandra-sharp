// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2018 Pierre Chalamet
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

using System.Collections.Generic;
using CassandraSharp.Extensibility;
using CassandraSharp.Utils.Stream;

namespace CassandraSharp.CQLBinaryProtocol.Queries
{
    internal sealed class SetKeyspaceQuery : Query<bool>
    {
        private readonly string _keyspace;

        public SetKeyspaceQuery(IConnection connection, string keyspace)
            : base(connection, ConsistencyLevel.ONE, ExecutionFlags.None)
        {
            _keyspace = keyspace;
        }

        protected override IEnumerable<bool> ReadFrame(IFrameReader frameReader)
        {
            var stream = frameReader.ReadOnlyStream;
            var resultOpcode = (ResultOpcode)stream.ReadInt();
            yield return resultOpcode == ResultOpcode.Prepared;
        }

        protected override void WriteFrame(IFrameWriter fw)
        {
            var cql = "USE " + _keyspace;
            var stream = fw.WriteOnlyStream;

            stream.WriteLongString(cql);
            stream.WriteUShort((ushort)ConsistencyLevel);
            stream.WriteByte(0);
            fw.SetMessageType(MessageOpcodes.Query);
        }

        protected override InstrumentationToken CreateInstrumentationToken()
        {
            var token = InstrumentationToken.Create(RequestType.Ready, ExecutionFlags.None);
            return token;
        }
    }
}