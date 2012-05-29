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
                    {typeof(int?), x => null != x ? new IntNameOrValue((int) x) : null},
                    {typeof(long), x => new LongNameOrValue((long) x)},
                    {typeof(long?), x => null != x ? new LongNameOrValue((long) x) : null},
                    {typeof(float), x => new FloatNameOrValue((float) x)},
                    {typeof(float?), x => null != x ? new FloatNameOrValue((float) x) : null},
                    {typeof(double), x => new DoubleNameOrValue((double) x)},
                    {typeof(double?), x => null != x ? new DoubleNameOrValue((double) x) : null},
                    {typeof(string), x => null != x ? new Utf8NameOrValue((string) x): null},
                    {typeof(DateTime), x => new DateTimeNameOrValue((DateTime) x)},
                    {typeof(DateTime?), x => null != x ? new DateTimeNameOrValue(((DateTime) x)) : null},
                    {typeof(byte[]), x => new ByteArrayNameOrValue((byte[]) x)},
                    {typeof(Decimal), x => null},
                    {typeof(Decimal?), x => null},
                    {typeof(Guid), x => new GuidNameOrValue((Guid) x)},
                    {typeof(Guid?), x => null != x ? new GuidNameOrValue((Guid) x) : null},
                };

        private static readonly Dictionary<Type, Func<byte[], INameOrValue>> _netType2NameOrValueFromByteArray =
            new Dictionary<Type, Func<byte[], INameOrValue>>
                {
                    {typeof(int), x => new IntNameOrValue(x)},
                    {typeof(int?), x => new IntNameOrValue(x)},
                    {typeof(long), x => new LongNameOrValue(x)},
                    {typeof(long?), x => new LongNameOrValue(x)},
                    {typeof(float), x => new FloatNameOrValue(x)},
                    {typeof(float?), x => new FloatNameOrValue(x)},
                    {typeof(double), x => new DoubleNameOrValue(x)},
                    {typeof(double?), x => new DoubleNameOrValue(x)},
                    {typeof(string), x => new Utf8NameOrValue(x)},
                    {typeof(DateTime), x => new DateTimeNameOrValue(x)},
                    {typeof(DateTime?), x => new DateTimeNameOrValue(x)},
                    {typeof(byte[]), x => new ByteArrayNameOrValue(x)},
                    {typeof(Decimal), x => null},
                    {typeof(Decimal?), x => null},
                    {typeof(Guid), x => new GuidNameOrValue(x)},
                    {typeof(Guid?), x => new GuidNameOrValue(x)},
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