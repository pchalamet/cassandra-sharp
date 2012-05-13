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

namespace CassandraSharp.NameOrValues
{
    using System;

    public class DoubleNameOrValue : NameOrValueBase<double>
    {
        public DoubleNameOrValue(double value)
            : base(value)
        {
        }

        public DoubleNameOrValue(byte[] value)
            : base(value)
        {
        }

        public override byte[] ToByteArray()
        {
            byte[] value = BitConverter.GetBytes(Value);
            Array.Reverse(value);
            return value;
        }

        protected override double FromByteArray(byte[] value)
        {
            byte[] buffer = new byte[value.Length];
            value.CopyTo(buffer, 0);
            Array.Reverse(buffer);
            double result = BitConverter.ToDouble(buffer, 0);
            return result;
        }
    }
}