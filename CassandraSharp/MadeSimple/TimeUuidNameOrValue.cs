// cassandra-sharp - a .NET client for Apache Cassandra
// Copyright (c) 2011-2012 Pierre Chalamet
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

namespace CassandraSharp.MadeSimple
{
    using System;
    using CassandraSharp.Utils;

    public class TimeUuidNameOrValue : NameOrValueBase<DateTime>
    {
        public TimeUuidNameOrValue(DateTime value)
            : base(value)
        {
        }

        public TimeUuidNameOrValue(byte[] value)
            : base(value)
        {
        }

        public override DateTime Value
        {
            set
            {
                if (value.Kind != DateTimeKind.Utc)
                {
                    throw new ApplicationException("UTC DateTime required");
                }

                base.Value = value;
            }
        }

        public static INameOrValue FromNullable(DateTime? obj)
        {
            return obj.HasValue
                       ? new TimeUuidNameOrValue(obj.Value)
                       : null;
        }

        public static INameOrValue FromNullableByteArray(byte[] buffer)
        {
            return null != buffer
                       ? new TimeUuidNameOrValue(buffer)
                       : null;
        }

        public override byte[] ConvertToByteArray()
        {
            Guid guid = GuidGenerator.GenerateTimeBasedGuid(Value);
            return guid.ToByteArray();
        }

        protected override DateTime ConvertFromByteArray(byte[] value)
        {
            Guid guid = new Guid(value);
            DateTime result = GuidGenerator.GetDateTime(guid);
            return result;
        }
    }
}