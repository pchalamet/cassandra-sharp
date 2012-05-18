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
    using System.Reflection;

    internal class ColumnDef
    {
        private readonly PropertyInfo _pi;
        private readonly FieldInfo _fi;

        public ColumnDef(string name, DataType dataType, bool isKeyComponent, int index, MemberInfo mi)
        {
            Name = name;
            DataType = dataType;
            IsKeyComponent = isKeyComponent;
            Index = index;

            if( mi.MemberType == MemberTypes.Property)
            {
                _pi = (PropertyInfo) mi;
            }
            else
            {
                _fi = (FieldInfo) mi;
            }
        }

        public string Name { get; set; }

        public DataType DataType { get; set; }

        public bool IsKeyComponent { get; set; }

        public int Index { get; set; }

        public object GetValue(object target)
        {
            return null != _pi
                       ? _pi.GetValue(target, null)
                       : _fi.GetValue(target);
        }
    }
}