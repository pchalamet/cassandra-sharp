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
                                                                                       {typeof(byte[]), CqlType.Blob},
                                                                                       {typeof(Decimal), CqlType.Decimal},
                                                                                       {typeof(Decimal?), CqlType.Decimal},
                                                                                       {typeof(Guid), CqlType.Uuid},
                                                                                       {typeof(Guid?), CqlType.Uuid},
                                                                                   };

        private static readonly Dictionary<CqlType, string> _dataType2CqlType = new Dictionary<CqlType, string>
                                                                                     {
                                                                                         {CqlType.Ascii, "ascii"},
                                                                                         {CqlType.BigInt, "bigint"},
                                                                                         {CqlType.Blob, "blob"},
                                                                                         {CqlType.Boolean, "boolean"},
                                                                                         {CqlType.Counter, "counter"},
                                                                                         {CqlType.Decimal, "decimal"},
                                                                                         {CqlType.Double, "double"},
                                                                                         {CqlType.Float, "float"},
                                                                                         {CqlType.Int, "int"},
                                                                                         {CqlType.Text, "text"},
                                                                                         {CqlType.Timestamp, "timestamp"},
                                                                                         {CqlType.Uuid, "uuid"},
                                                                                         {CqlType.Varchar, "varchar"},
                                                                                         {CqlType.Varint, "varint"},
                                                                                     };


        public static string ToCql(this CqlType @this)
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

                    CqlType cqlType = ca.CqlType;
                    if (CqlType.Auto == cqlType)
                    {
                        Type mit = mi.MemberType == MemberTypes.Property
                                       ? ((PropertyInfo) mi).PropertyType
                                       : ((FieldInfo) mi).FieldType;

                        cqlType = _netType2DataType[mit];
                    }

                    bool isKeyComponent = ca is KeyAttribute;

                    CompositeKeyAttribute cka = ca as CompositeKeyAttribute;
                    if( null != cka)
                    {
                        index = cka.Index;
                    }

                    ColumnDef columnDef = new ColumnDef(name, cqlType, isKeyComponent, index, mi);
                    yield return columnDef;
                }
            }
        }
    }
}