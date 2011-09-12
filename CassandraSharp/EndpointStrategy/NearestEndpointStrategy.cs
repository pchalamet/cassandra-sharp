// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
// limitations under the License.
namespace CassandraSharp.EndpointStrategy
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.CompilerServices;

    internal class NearestEndpointStrategy : IEndpointStrategy
    {
        private readonly List<Endpoint> _bannedEndpoints;

        private readonly List<Endpoint> _healthyEndpoints;

        public NearestEndpointStrategy(IEnumerable<Endpoint> endpoints)
        {
            // order endpoints by proximity
            _healthyEndpoints = (from endpoint in endpoints
                                 orderby endpoint.Proximity
                                 select endpoint).ToList();
            _bannedEndpoints = new List<Endpoint>();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public Endpoint Pick()
        {
            if (0 < _healthyEndpoints.Count)
            {
                return _healthyEndpoints[0];
            }

            return null;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Ban(Endpoint endPoint)
        {
            if (_healthyEndpoints.Remove(endPoint))
            {
                _bannedEndpoints.Add(endPoint);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Permit(Endpoint endPoint)
        {
            if (_bannedEndpoints.Remove(endPoint))
            {
                _healthyEndpoints.Add(endPoint);
            }
        }
    }
}