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
    using System.Linq;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils.Stream;

    internal class CqlQuery<T> : Query<T>
    {
        protected readonly string CQL;

        protected readonly IDataMapperFactory Factory;

        public CqlQuery(IConnection connection, string cql, IDataMapperFactory factory)
                : base(connection)
        {
            CQL = cql;
            Factory = factory;
        }

        protected override IEnumerable<T> CreateReader(IFrameReader frameReader)
        {
            return ReadRowSet(frameReader, Factory).Cast<T>();
        }

        protected override Action<IFrameWriter> CreateWriter()
        {
            Action<IFrameWriter> writer = fw =>
                {
                    Stream stream = fw.WriteOnlyStream;
                    stream.WriteLongString(CQL);
                    stream.WriteShort((short) ConsistencyLevel);
                    fw.SetMessageType(MessageOpcodes.Query);
                };
            return writer;
        }

        protected override InstrumentationToken CreateToken()
        {
            InstrumentationToken token = InstrumentationToken.Create(RequestType.Query, ExecutionFlags, CQL);
            return token;
        }

        private static IEnumerable<object> ReadRowSet(IFrameReader frameReader, IDataMapperFactory mapperFactory)
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
            ResultOpcode resultOpcode = (ResultOpcode) stream.ReadInt();
            switch (resultOpcode)
            {
                case ResultOpcode.Void:
                    yield break;

                case ResultOpcode.Rows:
                    IColumnSpec[] columnSpecs = ReadColumnSpec(frameReader);
                    foreach (object row in ReadRows(frameReader, columnSpecs, mapperFactory))
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

        private static IEnumerable<object> ReadRows(IFrameReader frameReader, IColumnSpec[] columnSpecs, IDataMapperFactory mapperFactory)
        {
            Stream stream = frameReader.ReadOnlyStream;
            int rowCount = stream.ReadInt();
            for (int rowIdx = 0; rowIdx < rowCount; ++rowIdx)
            {
                IInstanceBuilder instanceBuilder = mapperFactory.CreateBuilder();
                foreach (ColumnSpec colSpec in columnSpecs)
                {
                    byte[] rawData = stream.ReadByteArray();

                    object data = null;
                    if (null != rawData)
                    {
                        data = colSpec.Deserialize(rawData);
                    }

                    instanceBuilder.Set(colSpec, data);
                }

                yield return instanceBuilder.Build();
            }
        }

        protected static IColumnSpec[] ReadColumnSpec(IFrameReader frameReader)
        {
            Stream stream = frameReader.ReadOnlyStream;
            MetadataFlags flags = (MetadataFlags) stream.ReadInt();
            bool globalTablesSpec = 0 != (flags & MetadataFlags.GlobalTablesSpec);

            int colCount = stream.ReadInt();

            string keyspace = null;
            string table = null;
            if (globalTablesSpec)
            {
                keyspace = stream.ReadString();
                table = stream.ReadString();
            }

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
                ColumnType colType = (ColumnType) stream.ReadShort();
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
                        colValueType = (ColumnType) stream.ReadShort();
                        break;

                    case ColumnType.Map:
                        colKeyType = (ColumnType) stream.ReadShort();
                        colValueType = (ColumnType) stream.ReadShort();
                        break;
                }

                columnSpecs[colIdx] = new ColumnSpec(colIdx, colKeyspace, colTable, colName, colType, colCustom, colKeyType, colValueType);
            }

            return columnSpecs;
        }
    }
}