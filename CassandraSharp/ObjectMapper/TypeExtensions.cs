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

    public static class TypeExtensions
    {
        public static IEnumerable<MemberInfo> GetPublicMembers(this Type @this)
        {
            IEnumerable<MemberInfo> fields = @this.GetFields().AsEnumerable().Cast<MemberInfo>();
            IEnumerable<MemberInfo> properties = @this.GetProperties().AsEnumerable().Cast<MemberInfo>();

            return fields.Union(properties);
        }

        public static void SetDuckValue(this object @this, string name, object value)
        {
            FieldInfo fi = @this.GetType().GetField(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            if (null != fi)
            {
                fi.SetValue(@this, value);
            }
            else
            {
                PropertyInfo pi = @this.GetType().GetProperty(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                pi.SetValue(@this, value, null);
            }
        }

        public static object GetDuckValue(this object @this, string name)
        {
            FieldInfo fi = @this.GetType().GetField(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            if (null != fi)
            {
                return fi.GetValue(@this);
            }

            PropertyInfo pi = @this.GetType().GetProperty(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            return pi.GetValue(@this, null);
        }

        public static Type GetDuckType(this MemberInfo @this)
        {
            if (@this.MemberType == MemberTypes.Field)
            {
                FieldInfo fi = (FieldInfo) @this;
                return fi.FieldType;
            }

            PropertyInfo pi = (PropertyInfo) @this;
            return pi.PropertyType;
        }
    }
}