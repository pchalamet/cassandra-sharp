// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2013 Pierre Chalamet
// 
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

namespace CassandraSharp.Partitioner
{
    using System.IO;
    using System.Linq;
    using System.Numerics;
    using CassandraSharp.CQLBinaryProtocol;
    using CassandraSharp.Extensibility;

    internal abstract class PartitionerBase : IPartitioner
    {
        public BigInteger? ComputeToken(object[] partitionKey)
        {
            if (null == partitionKey || 0 == partitionKey.Length)
            {
                return null;
            }

            if (1 == partitionKey.Length)
            {
                ColumnType colType = partitionKey[0].GetType().ToColumnType();
                byte[] buffer = ValueSerialization.Serialize(colType, partitionKey[0]);
                return Hash(buffer, 0, buffer.Length);
            }

            var rawValues = new byte[partitionKey.Length][];
            for (int i = 0; i < partitionKey.Length; i++)
            {
                ColumnType colType = partitionKey[i].GetType().ToColumnType();
                rawValues[i] = ValueSerialization.Serialize(colType, partitionKey[i]);
            }

            int length = partitionKey.Length * 3 + rawValues.Sum(val => val.Length);
            using (var stream = new MemoryStream(length))
            {
                foreach (var rawValue in rawValues)
                {
                    //write length of composite key part as short
                    var len = (short) rawValue.Length;
                    stream.WriteByte((byte) (len >> 8));
                    stream.WriteByte((byte) (len));

                    //write value
                    stream.Write(rawValue, 0, len);

                    //write terminator byte
                    stream.WriteByte(0);
                }

                byte[] buffer = stream.GetBuffer();
                return Hash(buffer, 0, (int) stream.Length);
            }
        }

        protected abstract BigInteger? Hash(byte[] buffer, int offset, int len);
    }
}