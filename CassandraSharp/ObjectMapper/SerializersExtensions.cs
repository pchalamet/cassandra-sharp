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
    using CassandraSharp.MadeSimple;

    internal static class SerializersExtensions
    {
        private static readonly Dictionary<Type, Func<object, INameOrValue>> _netType2NameOrValueFromValue =
            new Dictionary<Type, Func<object, INameOrValue>>
                {
                    {typeof(int), x => new IntNameOrValue((int) x)},
                    {typeof(int?), x => IntNameOrValue.FromNullable((int?) x)},
                    {typeof(long), x => new LongNameOrValue((long) x)},
                    {typeof(long?), x => LongNameOrValue.FromNullable((long?) x)},
                    {typeof(float), x => new FloatNameOrValue((float) x)},
                    {typeof(float?), x => FloatNameOrValue.FromNullable((float?) x)},
                    {typeof(double), x => new DoubleNameOrValue((double) x)},
                    {typeof(double?), x => DoubleNameOrValue.FromNullable((double?) x)},
                    {typeof(string), x => Utf8NameOrValue.FromNullable((string) x)},
                    {typeof(DateTime), x => new DateTimeNameOrValue((DateTime) x)},
                    {typeof(DateTime?), x => DateTimeNameOrValue.FromNullable((DateTime?) x)},
                    {typeof(byte[]), x => ByteArrayNameOrValue.FromObject(x)},
                    {typeof(Decimal), x => null},
                    {typeof(Decimal?), x => null},
                    {typeof(Guid), x => new GuidNameOrValue((Guid) x)},
                    {typeof(Guid?), x => GuidNameOrValue.FromNullable((Guid?) x)},
                };

        private static readonly Dictionary<Type, Func<byte[], INameOrValue>> _netType2NameOrValueFromByteArray =
            new Dictionary<Type, Func<byte[], INameOrValue>>
                {
                    {typeof(int), x => IntNameOrValue.FromBuffer(x)},
                    {typeof(int?), x => IntNameOrValue.FromBuffer(x)},
                    {typeof(long), x => LongNameOrValue.FromBuffer(x)},
                    {typeof(long?), x => LongNameOrValue.FromBuffer(x)},
                    {typeof(float), x => FloatNameOrValue.FromBuffer(x)},
                    {typeof(float?), x => FloatNameOrValue.FromBuffer(x)},
                    {typeof(double), x => DoubleNameOrValue.FromBuffer(x)},
                    {typeof(double?), x => DoubleNameOrValue.FromBuffer(x)},
                    {typeof(string), x => Utf8NameOrValue.FromBuffer(x)},
                    {typeof(DateTime), x => DateTimeNameOrValue.FromBuffer(x)},
                    {typeof(DateTime?), x => DateTimeNameOrValue.FromBuffer(x)},
                    {typeof(byte[]), x => ByteArrayNameOrValue.FromBuffer(x)},
                    {typeof(Decimal), x => null},
                    {typeof(Decimal?), x => null},
                    {typeof(Guid), x => GuidNameOrValue.FromBuffer(x)},
                    {typeof(Guid?), x => GuidNameOrValue.FromBuffer(x)},
                };

        public static byte[] Serialize(this Type mit, object miv)
        {
            Func<object, INameOrValue> converter = _netType2NameOrValueFromValue[mit];
            INameOrValue nov = converter(miv);
            return nov.ToByteArray();
        }

        public static object Deserialize(this Type mit, byte[] value)
        {
            Func<byte[], INameOrValue> converter = _netType2NameOrValueFromByteArray[mit];
            object miv = converter(value).RawValue;
            return miv;
        }
    }
}