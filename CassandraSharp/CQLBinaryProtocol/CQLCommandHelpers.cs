// cassandra-sharp - the high performance .NET CQL 3 binary protocol client for Apache Cassandra
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
    using System.Security.Authentication;
    using CassandraSharp.Exceptions;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Instrumentation;

    internal static class CQLCommandHelpers
    {
        internal static IObservable<object> CreateQuery(IConnection connection, string cql, ConsistencyLevel cl, IDataMapperFactory factory,
                                                        ExecutionFlags executionFlags)
        {
            Action<IFrameWriter> writer = fw => WriteQueryRequest(fw, cql, cl, MessageOpcodes.Query);
            Func<IFrameReader, IEnumerable<object>> reader = fr => ReadRowSet(fr, factory);
            InstrumentationToken token = InstrumentationToken.Create(RequestType.Query, executionFlags, cql);
            Query query = new Query(writer, reader, token, connection);
            return query;
        }

        internal static IObservable<object> CreateReadyQuery(IConnection connection, string cqlVersion)
        {
            Action<IFrameWriter> writer = fw => WriteReady(fw, cqlVersion);
            Func<IFrameReader, IEnumerable<object>> reader = fr => new object[] {ReadReady(fr)};
            InstrumentationToken token = InstrumentationToken.Create(RequestType.Ready, ExecutionFlags.None);
            Query query = new Query(writer, reader, token, connection);
            return query;
        }

        internal static IObservable<object> CreateAuthenticateQuery(IConnection connection, string user, string password)
        {
            Action<IFrameWriter> writer = fw => WriteAuthenticate(fw, user, password);
            Func<IFrameReader, IEnumerable<object>> reader = fr =>
                {
                    ReadAuthenticate(fr);
                    return null;
                };

            // ReSharper disable ReturnValueOfPureMethodIsNotUsed
            InstrumentationToken token = InstrumentationToken.Create(RequestType.Authenticate, ExecutionFlags.None);
            Query query = new Query(writer, reader, token, connection);
            return query;
        }

        internal static IObservable<object> CreatePrepareQuery(IConnection connection, string cql, ExecutionFlags executionFlags)
        {
            Action<IFrameWriter> writer = fw => WritePrepareRequest(fw, cql);
            Func<IFrameReader, IEnumerable<object>> reader = fr => ReadPreparedQuery(fr, connection);
            InstrumentationToken token = InstrumentationToken.Create(RequestType.Prepare, executionFlags, cql);
            Query query = new Query(writer, reader, token, connection);
            return query;
        }

        internal static IObservable<object> CreateExecuteQuery(IConnection connection, byte[] id, IColumnSpec[] columnSpecs, ConsistencyLevel cl,
                                                               ExecutionFlags executionFlags, string cql,
                                                               IDataMapperFactory factory)
        {
            Action<IFrameWriter> writer = fw => WriteExecuteRequest(fw, id, columnSpecs, cl, factory);
            Func<IFrameReader, IEnumerable<object>> reader = fr => ReadRowSet(fr, factory);
            InstrumentationToken token = InstrumentationToken.Create(RequestType.Prepare, executionFlags, cql);
            Query query = new Query(writer, reader, token, connection);
            return query;
        }

        private static void WriteReady(IFrameWriter frameWriter, string cqlVersion)
        {
            Dictionary<string, string> options = new Dictionary<string, string>
                {
                        {"CQL_VERSION", cqlVersion}
                };
            frameWriter.WriteStringMap(options);
            frameWriter.SetMessageType(MessageOpcodes.Startup);
        }

        private static bool ReadReady(IFrameReader frameReader)
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

        private static void WriteOptions(IFrameWriter frameWriter)
        {
            frameWriter.SetMessageType(MessageOpcodes.Options);
        }

        private static void ReadOptions(IFrameReader frameReader)
        {
            if (frameReader.MessageOpcode != MessageOpcodes.Supported)
            {
                throw new UnknownResponseException(frameReader.MessageOpcode);
            }
        }

        private static void WriteAuthenticate(IFrameWriter frameWriter, string user, string password)
        {
            Dictionary<string, string> authParams = new Dictionary<string, string>
                {
                        {"username", user},
                        {"password", password}
                };
            frameWriter.WriteStringMap(authParams);

            frameWriter.SetMessageType(MessageOpcodes.Credentials);
        }

        private static void ReadAuthenticate(IFrameReader frameReader)
        {
            if (frameReader.MessageOpcode != MessageOpcodes.Ready)
            {
                throw new InvalidCredentialException();
            }
        }

        private static void WritePrepareRequest(IFrameWriter frameWriter, string cql)
        {
            frameWriter.WriteLongString(cql);
            frameWriter.SetMessageType(MessageOpcodes.Prepare);
        }

        private static IEnumerable<object> ReadPreparedQuery(IFrameReader frameReader, IConnection connection)
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
            frameWriter.SetMessageType(opcode);
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
                    object data = null != rawData
                                          ? colSpec.Deserialize(rawData)
                                          : null;

                    // FIXME: require to support Nullable<T>
                    if (null != data)
                    {
                        instanceBuilder.Set(colSpec, data);
                    }
                }

                yield return instanceBuilder.Build();
            }
        }

        private static IColumnSpec[] ReadColumnSpec(IFrameReader frameReader)
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
                if (!globalTablesSpec)
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
                        colValueType = (ColumnType) frameReader.ReadShort();
                        break;

                    case ColumnType.Map:
                        colKeyType = (ColumnType) frameReader.ReadShort();
                        colValueType = (ColumnType) frameReader.ReadShort();
                        break;
                }

                columnSpecs[colIdx] = new ColumnSpec(colIdx, colKeyspace, colTable, colName, colType, colCustom, colKeyType, colValueType);
            }

            return columnSpecs;
        }

        private static void WriteExecuteRequest(IFrameWriter frameWriter, byte[] id, IColumnSpec[] columnSpecs, ConsistencyLevel cl, IDataMapperFactory factory)
        {
            frameWriter.WriteShortByteArray(id);
            frameWriter.WriteShort((short) columnSpecs.Length);

            IDataSource dataSource = factory.DataSource;
            foreach (IColumnSpec columnSpec in columnSpecs)
            {
                object data = dataSource.Get(columnSpec);
                byte[] rawData = columnSpec.Serialize(data);
                frameWriter.WriteByteArray(rawData);
            }

            frameWriter.WriteShort((short) cl);
            frameWriter.SetMessageType(MessageOpcodes.Execute);
        }
    }
}