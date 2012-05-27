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
    using System.Reflection;

    internal class ColumnDef
    {
        private readonly FieldInfo _fi;

        private readonly PropertyInfo _pi;

        public ColumnDef(string name, CqlType cqlType, bool isKeyComponent, int index, MemberInfo mi)
        {
            Name = name;
            CqlType = cqlType;
            IsKeyComponent = isKeyComponent;
            Index = index;

            if (mi.MemberType == MemberTypes.Property)
            {
                _pi = (PropertyInfo) mi;
                NetType = _pi.PropertyType;
            }
            else
            {
                _fi = (FieldInfo) mi;
                NetType = _fi.FieldType;
            }
        }

        public string Name { get; private set; }

        public CqlType CqlType { get; private set; }

        public Type NetType { get; private set; }

        public bool IsKeyComponent { get; private set; }

        public int Index { get; private set; }

        public object GetValue(object target)
        {
            return null != _pi
                       ? _pi.GetValue(target, null)
                       : _fi.GetValue(target);
        }

        public void SetValue(object target, object value)
        {
            if (null != _pi)
            {
                _pi.SetValue(target, value, null);
            }
            else
            {
                _fi.SetValue(target, value);
            }
        }
    }
}