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

namespace CassandraSharp.ObjectMapper
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;

    internal class Schema
    {
        private static readonly Dictionary<Type, CqlType> _netType2DataType = new Dictionary<Type, CqlType>
                                                                                  {
                                                                                      {typeof(int), CqlType.Int},
                                                                                      {typeof(int?), CqlType.Int},
                                                                                      {typeof(long), CqlType.BigInt},
                                                                                      {typeof(long?), CqlType.BigInt},
                                                                                      {typeof(float), CqlType.Float},
                                                                                      {typeof(float?), CqlType.Float},
                                                                                      {typeof(double), CqlType.Double},
                                                                                      {typeof(double?), CqlType.Double},
                                                                                      {typeof(string), CqlType.Text},
                                                                                      {typeof(DateTime), CqlType.Timestamp},
                                                                                      {typeof(DateTime?), CqlType.Timestamp},
                                                                                      {typeof(byte[]), CqlType.Blob},
                                                                                      {typeof(Decimal), CqlType.Decimal},
                                                                                      {typeof(Decimal?), CqlType.Decimal},
                                                                                      {typeof(Guid), CqlType.Uuid},
                                                                                      {typeof(Guid?), CqlType.Uuid},
                                                                                  };

        private static readonly object _lock = new object();

        private static readonly Dictionary<Type, Schema> _cachedSchemas = new Dictionary<Type, Schema>();

        private Schema(Type schemaType)
        {
            SchemaType = schemaType;

            SchemaAttribute schemaAttribute = (SchemaAttribute) schemaType.GetCustomAttributes(typeof(SchemaAttribute), true).Single();
            Keyspace = schemaAttribute.Keyspace;
            Table = schemaAttribute.Name ?? schemaType.Name;
            CompactStorage = schemaAttribute.CompactStorage;

            IEnumerable<ColumnDef> allColumns = FindColumns(schemaType);
            NetName2ColumnDefs = allColumns.ToDictionary(x => x.NetName);
            CqlName2ColumnDefs = allColumns.ToDictionary(x => x.CqlName.ToLower());
        }

        public Type SchemaType { get; private set; }

        public string Keyspace { get; private set; }

        public string Table { get; private set; }

        public bool CompactStorage { get; private set; }

        public Dictionary<string, ColumnDef> CqlName2ColumnDefs { get; private set; }

        public Dictionary<string, ColumnDef> NetName2ColumnDefs { get; private set; }

        public static Schema FromCache(Type schemaType)
        {
            lock (_lock)
            {
                Schema schema;
                if (! _cachedSchemas.TryGetValue(schemaType, out schema))
                {
                    schema = new Schema(schemaType);
                    _cachedSchemas.Add(schemaType, schema);
                }

                return schema;
            }
        }

        private static IEnumerable<ColumnDef> FindColumns(Type t)
        {
            foreach (MemberInfo mi in t.GetPublicMembers())
            {
                string netName = mi.Name;
                ColumnAttribute ca = (ColumnAttribute) mi.GetCustomAttributes(typeof(ColumnAttribute), true).SingleOrDefault();

                if (null != ca)
                {
                    string cqlName = ca.Name ?? netName;
                    int index = 0;

                    CqlType cqlType = ca.CqlType;
                    if (CqlType.Auto == cqlType)
                    {
                        Type mit = mi.GetDuckType();
                        cqlType = _netType2DataType[mit];
                    }

                    bool isKeyComponent = ca is KeyAttribute;

                    CompositeKeyAttribute cka = ca as CompositeKeyAttribute;
                    if (null != cka)
                    {
                        index = cka.Index;
                    }

                    ColumnDef columnDef = new ColumnDef(mi.Name, cqlName, cqlType, isKeyComponent, index, mi);
                    yield return columnDef;
                }
            }
        }
    }
}