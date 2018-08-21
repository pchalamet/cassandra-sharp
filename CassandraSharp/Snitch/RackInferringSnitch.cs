// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2018 Pierre Chalamet
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

using System.Collections.Generic;
using System.Globalization;
using System.Net;
using CassandraSharp.Extensibility;

namespace CassandraSharp.Snitch
{
    internal sealed class RackInferringSnitch : IEndpointSnitch
    {
        public string GetRack(IPAddress endpoint)
        {
            var addrBytes = endpoint.GetAddressBytes();
            var dc = addrBytes[2].ToString(CultureInfo.InvariantCulture);
            return dc;
        }

        public string GetDatacenter(IPAddress endpoint)
        {
            var addrBytes = endpoint.GetAddressBytes();
            var dc = addrBytes[1].ToString(CultureInfo.InvariantCulture);
            return dc;
        }

        public List<IPAddress> GetSortedListByProximity(IPAddress address, IEnumerable<IPAddress> unsortedAddress)
        {
            var sortedAddress = new List<IPAddress>(unsortedAddress);
            sortedAddress.Sort((a1, a2) => CompareEndpoints(address, a1, a2));
            return sortedAddress;
        }

        public int CompareEndpoints(IPAddress address, IPAddress a1, IPAddress a2)
        {
            // compare address first
            if (Equals(address, a1) && !Equals(address, a2)) return -1;

            if (Equals(address, a2) && !Equals(address, a1)) return 1;

            // compare datacenter
            var addressDatacenter = GetDatacenter(address);
            var a1Datacenter = GetDatacenter(a1);
            var a2Datacenter = GetDatacenter(a2);

            if (addressDatacenter == a1Datacenter && addressDatacenter != a2Datacenter) return -1;

            if (addressDatacenter == a2Datacenter && addressDatacenter != a1Datacenter) return 1;

            // compare rack
            var addressRack = GetRack(address);
            var a1Rack = GetRack(a1);
            var a2Rack = GetRack(a2);

            if (addressRack == a1Rack && addressRack != a2Rack) return -1;
            if (addressRack == a2Rack && addressRack != a1Rack) return 1;

            return 0;
        }
    }
}