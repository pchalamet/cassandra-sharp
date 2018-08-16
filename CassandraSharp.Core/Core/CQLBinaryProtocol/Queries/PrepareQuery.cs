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

namespace CassandraSharp.CQLBinaryProtocol.Queries
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils.Stream;

    internal sealed class PrepareQuery : CqlQuery<Tuple<byte[], IColumnSpec[]>>
    {
        public PrepareQuery(IConnection connection, ConsistencyLevel consistencyLevel, ExecutionFlags executionFlags, string cql)
                : base(connection, consistencyLevel, executionFlags, cql, null)
        {
        }

        protected override IEnumerable<Tuple<byte[], IColumnSpec[]>> ReadFrame(IFrameReader frameReader)
        {
            if (MessageOpcodes.Result != frameReader.MessageOpcode)
            {
                throw new ArgumentException("Unknown server response");
            }

            Stream stream = frameReader.ReadOnlyStream;
            ResultOpcode resultOpcode = (ResultOpcode) stream.ReadInt();
            switch (resultOpcode)
            {
                case ResultOpcode.Prepared:
                    byte[] queryId = stream.ReadShortBytes();
                    IColumnSpec[] columnSpecs = ReadMetadata(stream);                
                    ReadResultMetadata(frameReader);
                    yield return Tuple.Create(queryId, columnSpecs);
                    break;

                default:
                    throw new ArgumentException("Unexpected ResultOpcode");
            }
        }

        private static IColumnSpec[] ReadMetadata(Stream stream)
        {
            var flags = (MetadataFlags)stream.ReadInt();
            var columnCount = stream.ReadInt();
            var pkColumnIndexCount = stream.ReadInt();

            // read primary key indices
            var pkIndices = new ushort[pkColumnIndexCount];
            for (int i = 0; i < pkColumnIndexCount; ++i)
            {
                pkIndices[i] = stream.ReadUShort();
            }

            // read column specs
            string keyspace = null;
            string table = null;
            bool globalTablesSpec = 0 != (flags & MetadataFlags.GlobalTablesSpec);
            if (globalTablesSpec)
            {
                keyspace = stream.ReadString();
                table = stream.ReadString();
            }

            var columnSpecs = ReadColumnSpecs(columnCount, keyspace, table, globalTablesSpec, stream);
            return columnSpecs;
        }

        protected override void WriteFrame(IFrameWriter fw)
        {
            Stream stream = fw.WriteOnlyStream;
            stream.WriteLongString(CQL);
            fw.SetMessageType(MessageOpcodes.Prepare);
        }

        protected override InstrumentationToken CreateInstrumentationToken()
        {
            InstrumentationToken token = InstrumentationToken.Create(RequestType.Prepare, ExecutionFlags, CQL);
            return token;
        }
    }
}