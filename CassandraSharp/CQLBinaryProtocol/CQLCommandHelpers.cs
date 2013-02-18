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

namespace CassandraSharp.CQLBinaryProtocol
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Security.Authentication;
    using CassandraSharp.Exceptions;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Instrumentation;
    using CassandraSharp.Utils.Stream;

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
            Func<IFrameReader, IEnumerable<object>> reader = ReadReady;
            InstrumentationToken token = InstrumentationToken.Create(RequestType.Ready, ExecutionFlags.None);
            Query query = new Query(writer, reader, token, connection);
            return query;
        }

        internal static IObservable<object> CreateOptionsQuery(IConnection connection)
        {
            Action<IFrameWriter> writer = WriteOptions;
            Func<IFrameReader, IEnumerable<object>> reader = ReadOptions;
            InstrumentationToken token = InstrumentationToken.Create(RequestType.Options, ExecutionFlags.None);
            Query query = new Query(writer, reader, token, connection);
            return query;
        }

        internal static IObservable<object> CreateAuthenticateQuery(IConnection connection, string user, string password)
        {
            Action<IFrameWriter> writer = fw => WriteAuthenticate(fw, user, password);
            Func<IFrameReader, IEnumerable<object>> reader = ReadAuthenticate;

            // ReSharper disable ReturnValueOfPureMethodIsNotUsed
            InstrumentationToken token = InstrumentationToken.Create(RequestType.Authenticate, ExecutionFlags.None);
            Query query = new Query(writer, reader, token, connection);
            return query;
        }

        internal static IObservable<object> CreatePrepareQuery(IConnection connection, string cql, ExecutionFlags executionFlags)
        {
            Action<IFrameWriter> writer = fw => WritePrepareRequest(fw, cql);
            Func<IFrameReader, IEnumerable<object>> reader = ReadPreparedQuery;
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
            frameWriter.WriteOnlyStream.WriteStringMap(options);
            frameWriter.SetMessageType(MessageOpcodes.Startup);
        }

        private static IEnumerable<object> ReadReady(IFrameReader frameReader)
        {
            bool mustAuthenticate = frameReader.MessageOpcode == MessageOpcodes.Authenticate;
            yield return mustAuthenticate;
        }

        private static void WriteOptions(IFrameWriter frameWriter)
        {
            frameWriter.SetMessageType(MessageOpcodes.Options);
        }

        private static IEnumerable<object> ReadOptions(IFrameReader frameReader)
        {
            if (frameReader.MessageOpcode != MessageOpcodes.Supported)
            {
                throw new UnknownResponseException(frameReader.MessageOpcode);
            }

            Stream stream = frameReader.ReadOnlyStream;
            Dictionary<string, string[]> res = stream.ReadStringMultimap();
            yield return res;
        }

        private static void WriteAuthenticate(IFrameWriter frameWriter, string user, string password)
        {
            Stream stream = frameWriter.WriteOnlyStream;
            Dictionary<string, string> authParams = new Dictionary<string, string>
                {
                        {"username", user},
                        {"password", password}
                };
            stream.WriteStringMap(authParams);
            frameWriter.SetMessageType(MessageOpcodes.Credentials);
        }

        private static IEnumerable<object> ReadAuthenticate(IFrameReader frameReader)
        {
            if (frameReader.MessageOpcode != MessageOpcodes.Ready)
            {
                throw new InvalidCredentialException();
            }

            return Enumerable.Empty<object>();
        }

        private static void WritePrepareRequest(IFrameWriter frameWriter, string cql)
        {
            Stream stream = frameWriter.WriteOnlyStream;
            stream.WriteLongString(cql);
            frameWriter.SetMessageType(MessageOpcodes.Prepare);
        }

        private static IEnumerable<object> ReadPreparedQuery(IFrameReader frameReader)
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
                    IColumnSpec[] columnSpecs = ReadColumnSpec(frameReader);
                    yield return Tuple.Create(queryId, columnSpecs);
                    break;

                default:
                    throw new ArgumentException("Unexpected ResultOpcode");
            }
        }

        private static void WriteQueryRequest(IFrameWriter frameWriter, string cql, ConsistencyLevel cl, MessageOpcodes opcode)
        {
            Stream stream = frameWriter.WriteOnlyStream;
            stream.WriteLongString(cql);
            stream.WriteShort((short) cl);
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
                    object data = null != rawData
                                          ? colSpec.Deserialize(rawData)
                                          : null;
                    instanceBuilder.Set(colSpec, data);
                }

                yield return instanceBuilder.Build();
            }
        }

        private static IColumnSpec[] ReadColumnSpec(IFrameReader frameReader)
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

        private static void WriteExecuteRequest(IFrameWriter frameWriter, byte[] id, IColumnSpec[] columnSpecs, ConsistencyLevel cl, IDataMapperFactory factory)
        {
            Stream stream = frameWriter.WriteOnlyStream;
            stream.WriteShortByteArray(id);
            stream.WriteShort((short) columnSpecs.Length);

            IDataSource dataSource = factory.DataSource;
            foreach (IColumnSpec columnSpec in columnSpecs)
            {
                object data = dataSource.Get(columnSpec);
                byte[] rawData = columnSpec.Serialize(data);
                stream.WriteByteArray(rawData);
            }

            stream.WriteShort((short) cl);
            frameWriter.SetMessageType(MessageOpcodes.Execute);
        }
    }
}