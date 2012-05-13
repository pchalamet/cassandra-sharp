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
    using System;

    public class FloatNameOrValue : NameOrValueBase<float>
    {
        public FloatNameOrValue(float value)
            : base(value)
        {
        }

        public FloatNameOrValue(byte[] value)
            : base(value)
        {
        }

        public override byte[] ToByteArray()
        {
            byte[] value = BitConverter.GetBytes(Value);
            Array.Reverse(value);
            return value;
        }

        protected override float FromByteArray(byte[] value)
        {
            byte[] buffer = new byte[value.Length];
            value.CopyTo(buffer, 0);
            Array.Reverse(buffer);
            float result = BitConverter.ToSingle(buffer, 0);
            return result;
        }
    }
}