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
    using System;
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;

    internal class RandomEndpointStrategy : IEndpointStrategy
    {
        private readonly List<Endpoint> _bannedEndpoints;

        private readonly List<Endpoint> _healthyEndpoints;

        private readonly Random _rnd;

        public RandomEndpointStrategy(IEnumerable<Endpoint> endpoints)
        {
            _healthyEndpoints = new List<Endpoint>(endpoints);
            _bannedEndpoints = new List<Endpoint>();
            _rnd = new Random();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Endpoint Pick(byte[] keyHint)
        {
            if (0 < _healthyEndpoints.Count)
            {
                int candidate = _rnd.Next(_healthyEndpoints.Count);
                Endpoint endpoint = _healthyEndpoints[candidate];
                return endpoint;
            }

            return null;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Ban(Endpoint endpoint)
        {
            if (_healthyEndpoints.Remove(endpoint))
            {
                _bannedEndpoints.Add(endpoint);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Permit(Endpoint endpoint)
        {
            if (_bannedEndpoints.Remove(endpoint))
            {
                _healthyEndpoints.Add(endpoint);
            }
        }
    }
}