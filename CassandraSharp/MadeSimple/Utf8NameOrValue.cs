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

namespace CassandraSharp.MadeSimple
{
    using System.Text;

    public class Utf8NameOrValue : NameOrValueBase<string>
    {
        public Utf8NameOrValue(string value)
            : base(value)
        {
        }

        public Utf8NameOrValue(byte[] value)
            : base(value)
        {
        }

        public static INameOrValue FromNullable(string obj)
        {
            return null != obj
                       ? new Utf8NameOrValue(obj)
                       : null;
        }

        public static INameOrValue FromBuffer(byte[] buffer)
        {
            return null != buffer
                       ? new Utf8NameOrValue(buffer)
                       : null;
        }

        public override byte[] ToByteArray()
        {
            byte[] result = Encoding.UTF8.GetBytes(Value);
            return result;
        }

        protected override string FromByteArray(byte[] value)
        {
            string result = Encoding.UTF8.GetString(value);
            return result;
        }
    }
}