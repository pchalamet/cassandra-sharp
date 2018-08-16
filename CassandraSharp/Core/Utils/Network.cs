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

namespace CassandraSharp.Utils
{
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;

    internal sealed class Network
    {
        public static bool IsValidEndpoint(IPAddress rpcAddress)
        {
            try
            {
                byte[] addrBytes = rpcAddress.GetAddressBytes();
                bool isValid = 0 != addrBytes.Max();
                return isValid;
            }
            catch
            {
                return false;
            }
        }

        public static IPAddress Find(string hostname)
        {
            IPAddress ipAddress;
            if (IPAddress.TryParse(hostname, out ipAddress))
            {
                return ipAddress;
            }

            try
            {
                ipAddress = (from address in Dns.GetHostAddresses(hostname)
                             where address.AddressFamily == AddressFamily.InterNetwork
                             select address).FirstOrDefault();
                return ipAddress;
            }
            catch
            {
                return null;
            }
        }
    }
}