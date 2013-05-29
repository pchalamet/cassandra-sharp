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

namespace CassandraSharp.Snitch
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using CassandraSharp.Extensibility;
    using System.Linq;

    /// <summary>
    /// Requires dc and rack attributes on all server config elements 
    /// </summary>
    internal sealed class PropertyFileSnitch : IEndpointSnitch
    {
        private readonly Dictionary<IPAddress, Topology> _networkTopology;

        /// <summary>
        /// Creates map of ip addresses to 
        /// </summary>
        /// <param name="endpoints">Endpoints using tuple (IP,datacentre,rack)</param>
        public PropertyFileSnitch(IEnumerable<Tuple<IPAddress,string,string>> endpoints)
        {
            _networkTopology = new Dictionary<IPAddress, Topology>();
            foreach (var endpoint in endpoints)
            {
                IPAddress ip = endpoint.Item1;
                _networkTopology.Add(ip, new Topology(endpoint.Item2, endpoint.Item3));

                if(string.IsNullOrEmpty(_networkTopology[ip].datacentre))
                    throw new ArgumentException("No datacentre supplied for: " + ip.ToString());

                if(string.IsNullOrEmpty(_networkTopology[ip].rack))
                    throw new ArgumentException("No rack supplied for: " + ip.ToString());
            }
        }

        private Topology GetTopology(IPAddress endpoint)
        {
            try
            {
                return _networkTopology[endpoint];
            }
            catch
            {
                throw new Exception("Unrecognised ip address: " + endpoint);
            }
        }

        public string GetRack(IPAddress endpoint)
        {
            return GetTopology(endpoint).rack;
        }

        public string GetDatacenter(IPAddress endpoint)
        {
            return GetTopology(endpoint).datacentre;
        }

        public List<IPAddress> GetSortedListByProximity(IPAddress address, IEnumerable<IPAddress> unsortedAddress)
        {
            var addressTopology = GetTopology(address);

            return unsortedAddress
                .OrderBy(ip => addressTopology.GetProximity(GetTopology(ip)))
                .ToList();
        }

        public int CompareEndpoints(IPAddress address, IPAddress a1, IPAddress a2)
        {
            var addressTopology = GetTopology(address);
            int proximityA1 = addressTopology.GetProximity(GetTopology(a1));
            int proximityA2 = addressTopology.GetProximity(GetTopology(a2));

            if (proximityA1 == proximityA2)
                return 0; //address is neither closer to a1 or a2
            else if (proximityA1 < proximityA2)
                return -1; //address is closer to a1
            else
                return 1; //address is closer to a2
        }

        private class Topology
        {
            public string datacentre { get; private set; }
            public string rack { get; private set; }

            public Topology(string datacentre, string rack)
            {
                this.datacentre = datacentre;
                this.rack = rack;
            }

            public override bool Equals(object obj)
            {
                if(obj.GetType() != typeof(Topology))
                    return false;
                Topology o = (Topology)obj;
                return o.datacentre == this.datacentre
                    && o.rack == this.rack;
            }

            /// <summary>
            /// Get's a proximity score where
            /// 0 = same rack
            /// 1 = same datacentre
            /// 2 = different datacentres
            /// </summary>
            /// <param name="obj"></param>
            /// <returns></returns>
            public int GetProximity(object obj)
            {
                if(this.Equals(obj)) return 0;

                Topology o = (Topology)obj;
                if (this.datacentre == o.datacentre)
                    //same dc differnt rack
                    return 1;
                else
                    //different dc, rack irrelevent
                    return 2;
            }
        }
    }
}