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
    using System.Text;
    using CassandraSharp.MadeSimple;

    public static class SchemaExtensions
    {
        private static readonly Dictionary<Type, DataType> _netType2DataType = new Dictionary<Type, DataType>
                                                                                   {
                                                                                       {typeof(int), DataType.Int},
                                                                                       {typeof(int?), DataType.Int},
                                                                                       {typeof(long), DataType.BigInt},
                                                                                       {typeof(long?), DataType.BigInt},
                                                                                       {typeof(float), DataType.Float},
                                                                                       {typeof(float?), DataType.Float},
                                                                                       {typeof(double), DataType.Double},
                                                                                       {typeof(double?), DataType.Double},
                                                                                       {typeof(string), DataType.Text},
                                                                                       {typeof(DateTime), DataType.Timestamp},
                                                                                       {typeof(byte[]), DataType.Blob},
                                                                                       {typeof(Decimal), DataType.Decimal},
                                                                                       {typeof(Decimal?), DataType.Decimal},
                                                                                       {typeof(Guid), DataType.Uuid},
                                                                                       {typeof(Guid?), DataType.Uuid},
                                                                                   };

        private static readonly Dictionary<DataType, string> _dataType2CqlType = new Dictionary<DataType, string>
                                                                                     {
                                                                                         {DataType.Ascii, "ascii"},
                                                                                         {DataType.BigInt, "bigint"},
                                                                                         {DataType.Blob, "blob"},
                                                                                         {DataType.Boolean, "boolean"},
                                                                                         {DataType.Counter, "counter"},
                                                                                         {DataType.Decimal, "decimal"},
                                                                                         {DataType.Double, "double"},
                                                                                         {DataType.Float, "float"},
                                                                                         {DataType.Int, "int"},
                                                                                         {DataType.Text, "text"},
                                                                                         {DataType.Timestamp, "timestamp"},
                                                                                         {DataType.Uuid, "uuid"},
                                                                                         {DataType.Varchar, "varchar"},
                                                                                         {DataType.Varint, "varint"},
                                                                                     };

        private static SchemaAttribute FindSchemaAttribute(Type t)
        {
            SchemaAttribute sa = (SchemaAttribute) t.GetCustomAttributes(typeof(SchemaAttribute), true).Single();
            if (null == sa.Name)
            {
                sa.Name = t.Name;
            }

            return sa;
        }

        private static IEnumerable<ColumnAttribute> FindColumns(Type t)
        {
            foreach (MemberInfo mi in t.GetMembers())
            {
                ColumnAttribute ca = (ColumnAttribute) mi.GetCustomAttributes(typeof(ColumnAttribute), true).SingleOrDefault();
                if (null != ca)
                {
                    if (null == ca.Name)
                    {
                        ca.Name = mi.Name;
                    }

                    if (ca.DataType == DataType.Auto)
                    {
                        Type mit = mi.MemberType == MemberTypes.Property
                                       ? ((PropertyInfo) mi).PropertyType
                                       : ((FieldInfo) mi).FieldType;

                        ca.DataType = _netType2DataType[mit];
                    }

                    yield return ca;
                }
            }
        }

        public static void Create<T>(this ICluster cluster)
        {
            Type t = typeof(T);

            SchemaAttribute schemaAttribute = FindSchemaAttribute(t);
            IEnumerable<ColumnAttribute> allColumns = FindColumns(t);

            ColumnAttribute keyAttribute = allColumns.Single(x => x is KeyAttribute);
            IEnumerable<IndexAttribute> indices = allColumns.Where(x => x is IndexAttribute).Cast<IndexAttribute>();
            IEnumerable<ColumnAttribute> columns = allColumns.Where(x => !(x is KeyAttribute));

            StringBuilder sbCreateTable = new StringBuilder();
            string tableName = schemaAttribute.Name ?? t.Name;
            string keyName = keyAttribute.Name;
            string keyDataType = _dataType2CqlType[keyAttribute.DataType];

            BehaviorConfigBuilder cfgBuilder = new BehaviorConfigBuilder();
            cfgBuilder.KeySpace = schemaAttribute.Keyspace;
            ICluster tmpCluster = cluster.Configure(cfgBuilder);

            // create table first
            sbCreateTable.AppendFormat("create table {0} ({1} {2}", tableName, keyName, keyDataType);
            foreach (ColumnAttribute ca in columns)
            {
                string colDataType = _dataType2CqlType[ca.DataType];
                sbCreateTable.AppendFormat(", {0} {1}", ca.Name, colDataType);
            }
            sbCreateTable.AppendFormat(", primary key ({0}) );", keyName);

            string createTableStmt = sbCreateTable.ToString();
            tmpCluster.ExecuteCql(createTableStmt);

            // create indices then
            foreach (IndexAttribute ia in indices)
            {
                StringBuilder sbCreateIndex = new StringBuilder();
                sbCreateIndex.AppendFormat("create index on '{0}'('{1}')", tableName, ia.Name);

                string createIndexStmt = sbCreateIndex.ToString();
                tmpCluster.ExecuteCql(createIndexStmt);
            }
        }

        public static void Drop<T>(this ICluster cluster)
        {
            Type t = typeof(T);

            SchemaAttribute schemaAttribute = FindSchemaAttribute(t);

            StringBuilder sbDropTable = new StringBuilder();
            string tableName = schemaAttribute.Name ?? t.Name;
            sbDropTable.AppendFormat("drop columnfamily '{0}'", tableName);

            BehaviorConfigBuilder cfgBuilder = new BehaviorConfigBuilder {KeySpace = schemaAttribute.Keyspace};
            ICluster tmpCluster = cluster.Configure(cfgBuilder);

            string dropTableStmt = sbDropTable.ToString();
            tmpCluster.ExecuteCql(dropTableStmt);
        }

        public static void Truncate<T>(this ICluster cluster)
        {
            Type t = typeof(T);

            SchemaAttribute schemaAttribute = FindSchemaAttribute(t);

            StringBuilder sbTruncateTable = new StringBuilder();
            string tableName = schemaAttribute.Name ?? t.Name;
            sbTruncateTable.AppendFormat("truncate '{0}'", tableName);

            BehaviorConfigBuilder cfgBuilder = new BehaviorConfigBuilder();
            cfgBuilder.KeySpace = schemaAttribute.Keyspace;
            ICluster tmpCluster = cluster.Configure(cfgBuilder);

            string dropTableStmt = sbTruncateTable.ToString();
            tmpCluster.ExecuteCql(dropTableStmt);
        }
    }
}