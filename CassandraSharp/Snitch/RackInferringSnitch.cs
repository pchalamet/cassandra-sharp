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
namespace CassandraSharp.Snitch
{
    using System.Net;

    internal class RackInferringSnitch : ISnitch
    {
        public string GetDataCenter(IPAddress target)
        {
            byte[] addrBytes = target.GetAddressBytes();
            string dc = addrBytes[1].ToString();
            return dc;
        }

        public int ComputeDistance(IPAddress source, IPAddress target)
        {
            byte[] addrBytes = source.GetAddressBytes();
            int sourceNum = (addrBytes[1] << 16) + addrBytes[2];

            addrBytes = target.GetAddressBytes();
            int targetNum = (addrBytes[1] << 16) + addrBytes[2];

            int distance = targetNum - sourceNum;
            return distance;
        }
    }
}