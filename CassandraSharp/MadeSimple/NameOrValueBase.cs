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
    public abstract class NameOrValueBase<T> : INameOrValue
    {
        protected NameOrValueBase(byte[] value)
        {
// ReSharper disable DoNotCallOverridableMethodsInConstructor
            Value = ConvertFromByteArray(value);
// ReSharper restore DoNotCallOverridableMethodsInConstructor
        }

        protected NameOrValueBase(T value)
        {
// ReSharper disable DoNotCallOverridableMethodsInConstructor
            Value = value;
// ReSharper restore DoNotCallOverridableMethodsInConstructor
        }

        public virtual T Value { get; set; }

        public object RawValue
        {
            get { return Value; }
        }

        public abstract byte[] ConvertToByteArray();

        protected abstract T ConvertFromByteArray(byte[] value);
    }
}