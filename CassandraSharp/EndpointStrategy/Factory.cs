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

namespace CassandraSharp.EndpointStrategy
{
    using System.Collections.Generic;
    using CassandraSharp.Utils;

    internal static class Factory
    {
        public static IEndpointStrategy Create(string customType, IEnumerable<Endpoint> endpoints)
        {
            switch (customType)
            {
                case "Random":
                    return new RandomEndpointStrategy(endpoints);

                case "Nearest":
                    return new NearestEndpointStrategy(endpoints);

                default:
                    return ServiceActivator.Create<IEndpointStrategy>(customType, endpoints);
            }
        }
    }
}