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

namespace CassandraSharp.Transport
{
    using System.Net;
    using CassandraSharp.Config;
    using Thrift.Transport;

    internal class BufferedTransportFactory : BaseTransportFactory
    {
        private readonly int _port;

        private readonly int _timeout;

        public BufferedTransportFactory(TransportConfig config)
            : base(config)
        {
            _port = config.Port;
            _timeout = config.Timeout;
        }

        protected override TTransport CreateTransport(IPAddress address)
        {
            string ip = address.ToString();
            TStreamTransport streamTransport = new TSocket(ip, _port, _timeout);
            TTransport framedTransport = new TBufferedTransport(streamTransport);
            return framedTransport;
        }
    }
}