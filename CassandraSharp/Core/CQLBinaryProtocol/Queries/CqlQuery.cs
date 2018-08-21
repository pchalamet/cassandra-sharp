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

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CassandraSharp.Core.Utils.Stream;
using CassandraSharp.Extensibility;

namespace CassandraSharp.Core.CQLBinaryProtocol.Queries
{
    internal class CqlQuery<T> : Query<T>
    {
        protected readonly string CQL;

        private readonly IDataMapper _mapperOut;

        public CqlQuery(IConnection connection, ConsistencyLevel consistencyLevel, ExecutionFlags executionFlags, string cql, IDataMapper mapperOut)
            : base(connection, consistencyLevel, executionFlags)
        {
            CQL = cql;
            _mapperOut = mapperOut;
        }

        protected override IEnumerable<T> ReadFrame(IFrameReader frameReader)
        {
            return ReadRowSet(frameReader, _mapperOut);
        }

        protected override void WriteFrame(IFrameWriter fw)
        {
            Stream stream = fw.WriteOnlyStream;
            stream.WriteLongString(CQL);
            stream.WriteUShort((ushort)ConsistencyLevel);
            stream.WriteByte(0);
            fw.SetMessageType(MessageOpcodes.Query);
        }

        protected override InstrumentationToken CreateInstrumentationToken()
        {
            InstrumentationToken token = InstrumentationToken.Create(RequestType.Query, ExecutionFlags, CQL);
            return token;
        }

        private static IEnumerable<T> ReadRowSet(IFrameReader frameReader, IDataMapper mapperFactory)
        {
            if (MessageOpcodes.Result != frameReader.MessageOpcode)
            {
                throw new ArgumentException("Unknown server response");
            }

            if (null == mapperFactory)
            {
                yield break;
            }

            Stream stream = frameReader.ReadOnlyStream;
            ResultOpcode resultOpcode = (ResultOpcode)stream.ReadInt();
            switch (resultOpcode)
            {
                case ResultOpcode.Void:
                    yield break;

                case ResultOpcode.Rows:
                    IColumnSpec[] columnSpecs = ReadResultMetadata(frameReader);
                    foreach (T row in ReadRows(frameReader, columnSpecs, mapperFactory))
                    {
                        yield return row;
                    }
                    break;

                case ResultOpcode.SetKeyspace:
                    yield break;

                case ResultOpcode.SchemaChange:
                    yield break;

                default:
                    throw new ArgumentException("Unexpected ResultOpcode");
            }
        }

        private static IEnumerable<T> ReadRows(IFrameReader frameReader, IColumnSpec[] columnSpecs, IDataMapper mapperFactory)
        {
            Stream stream = frameReader.ReadOnlyStream;
            int rowCount = stream.ReadInt();
            for (int rowIdx = 0; rowIdx < rowCount; ++rowIdx)
            {
                var rowData = columnSpecs.Select(spec => new ColumnData(spec, stream.ReadBytesArray()));
                yield return (T)mapperFactory.MapToObject(rowData);
            }
        }

        protected static IColumnSpec[] ReadResultMetadata(IFrameReader frameReader)
        {
            Stream stream = frameReader.ReadOnlyStream;
            MetadataFlags flags = (MetadataFlags)stream.ReadInt();
            int colCount = stream.ReadInt();

            bool globalTablesSpec = 0 != (flags & MetadataFlags.GlobalTablesSpec);
            bool hasMorePages = 0 != (flags & MetadataFlags.HasMorePages);
            bool noMetadata = 0 != (flags & MetadataFlags.NoMetadata);

            string keyspace = null;
            string table = null;
            if (globalTablesSpec && !noMetadata)
            {
                keyspace = stream.ReadString();
                table = stream.ReadString();
            }

            if (hasMorePages)
            {
                var pagingState = stream.ReadBytesArray();
            }

            if (noMetadata)
                return null;

            var columnSpecs = ReadColumnSpecs(colCount, keyspace, table, globalTablesSpec, stream);
            return columnSpecs;
        }

        protected static IColumnSpec[] ReadColumnSpecs(int colCount, string keyspace, string table, bool globalTablesSpec, Stream stream)
        {
            IColumnSpec[] columnSpecs = new IColumnSpec[colCount];
            for (int colIdx = 0; colIdx < colCount; ++colIdx)
            {
                string colKeyspace = keyspace;
                string colTable = table;
                if (!globalTablesSpec)
                {
                    colKeyspace = stream.ReadString();
                    colTable = stream.ReadString();
                }

                string colName = stream.ReadString();
                ColumnType colType = (ColumnType)stream.ReadUShort();
                string colCustom = null;
                ColumnType colKeyType = ColumnType.Custom;
                ColumnType colValueType = ColumnType.Custom;
                switch (colType)
                {
                    case ColumnType.Custom:
                        colCustom = stream.ReadString();
                        break;

                    case ColumnType.List:
                    case ColumnType.Set:
                        colValueType = (ColumnType)stream.ReadUShort();
                        break;

                    case ColumnType.Map:
                        colKeyType = (ColumnType)stream.ReadUShort();
                        colValueType = (ColumnType)stream.ReadUShort();
                        break;
                }

                columnSpecs[colIdx] = new ColumnSpec(colIdx, colKeyspace, colTable, colName, colType, colCustom, colKeyType, colValueType);
            }
            return columnSpecs;
        }
    }
}