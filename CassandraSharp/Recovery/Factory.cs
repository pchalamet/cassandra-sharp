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

namespace CassandraSharp.Recovery
{
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;

    internal static class Factory
    {
        public static IRecoveryService Create(string customType, params object[] prms)
        {
            if (customType == "Null")
            {
                customType = ServiceActivator.GetTypeName<NullRecoveryService>();
            }
            else if (customType == "Simple")
            {
                customType = ServiceActivator.GetTypeName<SimpleRecoveryService>();
            }

            return ServiceActivator.Create<IRecoveryService>(customType, prms);
        }
    }
}