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

    internal static class TypeExtensions
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


        public static string ToCql(this DataType @this)
        {
            return _dataType2CqlType[@this];
        }


        public static SchemaAttribute FindSchemaAttribute(this Type t)
        {
            SchemaAttribute sa = (SchemaAttribute) t.GetCustomAttributes(typeof(SchemaAttribute), true).Single();
            if (null == sa.Name)
            {
                sa.Name = t.Name;
            }

            return sa;
        }

        public static IEnumerable<ColumnDef> FindColumns(this Type t)
        {
            foreach (MemberInfo mi in t.GetMembers())
            {
                ColumnAttribute ca = (ColumnAttribute) mi.GetCustomAttributes(typeof(ColumnAttribute), true).SingleOrDefault();

                int index = 0;

                if (null != ca)
                {
                    string name = ca.Name ?? mi.Name;

                    DataType dataType = ca.DataType;
                    if (DataType.Auto == dataType)
                    {
                        Type mit = mi.MemberType == MemberTypes.Property
                                       ? ((PropertyInfo) mi).PropertyType
                                       : ((FieldInfo) mi).FieldType;

                        dataType = _netType2DataType[mit];
                    }

                    bool isKeyComponent = ca is KeyAttribute;

                    CompositeKeyAttribute cka = ca as CompositeKeyAttribute;
                    if( null != cka)
                    {
                        index = cka.Index;
                    }

                    ColumnDef columnDef = new ColumnDef(name, dataType, isKeyComponent, index, mi);
                    yield return columnDef;
                }
            }
        }
    }
}