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

using System;

namespace CassandraSharp.Utils
{
    internal enum GuidVersion
    {
        TimeBased = 0x01,

        Reserved = 0x02,

        NameBased = 0x03,

        Random = 0x04
    }

    public static class TimedUuid
    {
        // number of bytes in guid
        private const int BYTE_ARRAY_SIZE = 16;

        // multiplex variant info
        private const int VARIANT_BYTE = 8;

        private const int VARIANT_BYTE_MASK = 0x3f;

        private const int VARIANT_BYTE_SHIFT = 0x80;

        // multiplex version info
        private const int VERSION_BYTE = 7;

        private const int VERSION_BYTE_MASK = 0x0f;

        private const int VERSION_BYTE_SHIFT = 4;

        // indexes within the uuid array for certain boundaries
        private const byte TIMESTAMP_BYTE = 0;

        private const byte GUID_CLOCK_SEQUENCE_BYTE = 8;

        private const byte NODE_BYTE = 10;

        // offset to move from 1/1/0001, which is 0-time for .NET, to gregorian 0-time of 10/15/1582
        private static readonly DateTime _gregorianCalendarStart = new DateTime(1582, 10, 15, 0, 0, 0, DateTimeKind.Utc);

        // random node that is 16 bytes
        private static readonly byte[] _randomNode;

        private static readonly Random _random = new Random();

        static TimedUuid()
        {
            _randomNode = new byte[6];
            _random.NextBytes(_randomNode);
        }

        public static DateTime GetDateTime(Guid guid)
        {
            byte[] bytes = guid.ToByteArray();

            // reverse the version
            bytes[VERSION_BYTE] &= VERSION_BYTE_MASK;
            bytes[VERSION_BYTE] |= (byte) GuidVersion.TimeBased >> VERSION_BYTE_SHIFT;

            byte[] timestampBytes = new byte[8];
            Array.Copy(bytes, TIMESTAMP_BYTE, timestampBytes, 0, 8);

            long timestamp = BitConverter.ToInt64(timestampBytes, 0);
            long ticks = timestamp + _gregorianCalendarStart.Ticks;

            return new DateTime(ticks, DateTimeKind.Utc);
        }

        public static Guid GenerateTimeBasedGuid(DateTime dateTime)
        {
            return GenerateTimeBasedGuid(dateTime, _randomNode);
        }

        private static Guid GenerateTimeBasedGuid(DateTime dateTime, byte[] node)
        {
            dateTime = dateTime.ToUniversalTime();
            long ticks = (dateTime - _gregorianCalendarStart).Ticks;

            byte[] guid = new byte[BYTE_ARRAY_SIZE];
            byte[] clockSequenceBytes = BitConverter.GetBytes(Convert.ToInt16(Environment.TickCount % Int16.MaxValue));
            byte[] timestamp = BitConverter.GetBytes(ticks);

            // copy node
            Array.Copy(node, 0, guid, NODE_BYTE, Math.Min(6, node.Length));

            // copy clock sequence
            Array.Copy(clockSequenceBytes, 0, guid, GUID_CLOCK_SEQUENCE_BYTE, Math.Min(2, clockSequenceBytes.Length));

            // copy timestamp
            Array.Copy(timestamp, 0, guid, TIMESTAMP_BYTE, Math.Min(8, timestamp.Length));

            // set the variant
            guid[VARIANT_BYTE] &= VARIANT_BYTE_MASK;
            guid[VARIANT_BYTE] |= VARIANT_BYTE_SHIFT;

            // set the version
            guid[VERSION_BYTE] &= VERSION_BYTE_MASK;
            guid[VERSION_BYTE] |= (byte) GuidVersion.TimeBased << VERSION_BYTE_SHIFT;

            return new Guid(guid);
        }
    }
}