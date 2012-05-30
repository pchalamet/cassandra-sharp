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

namespace TestClient
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using CassandraSharp;

    public class CustomEndpointStrategy : IEndpointStrategy
    {
        private readonly IEnumerable<IPAddress> _endpoints;

        public CustomEndpointStrategy(IEnumerable<IPAddress> endpoints, IEndpointSnitch snitch)
        {
            _endpoints = endpoints;
        }

        public IPAddress Pick(Token token)
        {
            return _endpoints.FirstOrDefault();
        }

        public void Ban(IPAddress endpoint)
        {
        }

        public void Permit(IPAddress endpoint)
        {
        }
    }
}