// cassandra-sharp - a .NET client for Apache Cassandra
// Copyright (c) 2011-2012 Pierre Chalamet
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
    using System.Security.Authentication;
    using System.Threading.Tasks;
    using CassandraSharp.Exceptions;
    using CassandraSharp.Extensibility;

    internal static class CQLCommandHelpers
    {
        public static Task<IEnumerable<T>> Query<T>(ICluster cluster, string cql, ConsistencyLevel cl, IDataMapperFactory factory)
        {
            Action<IFrameWriter> writer = fw => WriteQueryRequest(fw, cql, cl, MessageOpcodes.Query);
            Func<IFrameReader, IEnumerable<object>> reader = fr => ReadRowSet(fr, factory);

            return cluster.GetConnection(null).Execute(writer, reader).ContinueWith(res => res.Result.Cast<T>());
        }

        internal static void WriteReady(IFrameWriter frameWriter, string cqlVersion)
        {
            Dictionary<string, string> options = new Dictionary<string, string>
                    {
                            {"CQL_VERSION", cqlVersion}
                    };
            frameWriter.WriteStringMap(options);
            frameWriter.Send(MessageOpcodes.Startup);
        }

        internal static bool ReadReady(IFrameReader frameReader)
        {
            switch (frameReader.MessageOpcode)
            {
                case MessageOpcodes.Ready:
                    return false;

                case MessageOpcodes.Authenticate:
                    return true;

                default:
                    throw new UnknownResponseException(frameReader.MessageOpcode);
            }
        }

        internal static void WriteOptions(IFrameWriter frameWriter)
        {
            frameWriter.Send(MessageOpcodes.Options);
        }

        internal static void ReadOptions(IFrameReader frameReader)
        {
            if (frameReader.MessageOpcode != MessageOpcodes.Supported)
            {
                throw new UnknownResponseException(frameReader.MessageOpcode);
            }
        }

        internal static void WriteAuthenticate(IFrameWriter frameWriter, string user, string password)
        {
            Dictionary<string, string> authParams = new Dictionary<string, string>
                {
                        {"username", user},
                        {"password", password}
                };
            frameWriter.WriteStringMap(authParams);

            frameWriter.Send(MessageOpcodes.Credentials);            
        }

        internal static void ReadAuthenticate(IFrameReader frameReader)
        {
            if (frameReader.MessageOpcode != MessageOpcodes.Ready)
            {
                throw new InvalidCredentialException();
            }            
        }

        internal static void WritePrepareRequest(IFrameWriter frameWriter, string cql)
        {
            frameWriter.WriteLongString(cql);
            frameWriter.Send(MessageOpcodes.Prepare);
        }

        internal static IEnumerable<object> ReadPreparedQuery(IFrameReader frameReader, IConnection connection)
        {
            if (MessageOpcodes.Result != frameReader.MessageOpcode)
            {
                throw new ArgumentException("Unknown server response");
            }

            ResultOpcode resultOpcode = (ResultOpcode) frameReader.ReadInt();
            switch (resultOpcode)
            {
                case ResultOpcode.Prepared:
                    byte[] queryId = frameReader.ReadShortBytes();
                    IColumnSpec[] columnSpecs = ReadColumnSpec(frameReader);
                    yield return Tuple.Create(queryId, columnSpecs);
                    break;

                default:
                    throw new ArgumentException("Unexpected ResultOpcode");
            }
        }

        private static void WriteQueryRequest(IFrameWriter frameWriter, string cql, ConsistencyLevel cl, MessageOpcodes opcode)
        {
            frameWriter.WriteLongString(cql);
            frameWriter.WriteShort((short) cl);
            frameWriter.Send(opcode);
        }

        internal static IEnumerable<object> ReadRowSet(IFrameReader frameReader, IDataMapperFactory mapperFactory)
        {
            if (MessageOpcodes.Result != frameReader.MessageOpcode)
            {
                throw new ArgumentException("Unknown server response");
            }

            if (null == mapperFactory)
            {
                yield break;
            }

            ResultOpcode resultOpcode = (ResultOpcode) frameReader.ReadInt();
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
            int rowCount = frameReader.ReadInt();
            for (int rowIdx = 0; rowIdx < rowCount; ++rowIdx)
            {
                IInstanceBuilder instanceBuilder = mapperFactory.CreateBuilder();
                foreach (ColumnSpec colSpec in columnSpecs)
                {
                    byte[] rawData = frameReader.ReadBytes();
                    if (null != rawData)
                    {
                        object data = colSpec.Deserialize(rawData);
                        instanceBuilder.Set(colSpec, data);
                    }
                }

                yield return instanceBuilder.Build();
            }
        }

        internal static IColumnSpec[] ReadColumnSpec(IFrameReader frameReader)
        {
            MetadataFlags flags = (MetadataFlags) frameReader.ReadInt();
            bool globalTablesSpec = 0 != (flags & MetadataFlags.GlobalTablesSpec);

            int colCount = frameReader.ReadInt();

            string keyspace = null;
            string table = null;
            if (globalTablesSpec)
            {
                keyspace = frameReader.ReadString();
                table = frameReader.ReadString();
            }

            IColumnSpec[] columnSpecs = new IColumnSpec[colCount];
            for (int colIdx = 0; colIdx < colCount; ++colIdx)
            {
                string colKeyspace = keyspace;
                string colTable = table;
                if (! globalTablesSpec)
                {
                    colKeyspace = frameReader.ReadString();
                    colTable = frameReader.ReadString();
                }
                string colName = frameReader.ReadString();
                ColumnType colType = (ColumnType) frameReader.ReadShort();
                string colCustom = null;
                ColumnType colKeyType = ColumnType.Custom;
                ColumnType colValueType = ColumnType.Custom;
                switch (colType)
                {
                    case ColumnType.Custom:
                        colCustom = frameReader.ReadString();
                        break;

                    case ColumnType.List:
                    case ColumnType.Set:
                        colValueType = (ColumnType)frameReader.ReadShort();
                        break;

                    case ColumnType.Map:
                        colKeyType = (ColumnType)frameReader.ReadShort();
                        colValueType = (ColumnType)frameReader.ReadShort();
                        break;
                }

                columnSpecs[colIdx] = new ColumnSpec(colIdx, colKeyspace, colTable, colName, colType, colCustom, colKeyType, colValueType);
            }

            return columnSpecs;
        }
    }
}