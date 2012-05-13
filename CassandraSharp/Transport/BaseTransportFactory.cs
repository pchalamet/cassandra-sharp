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
    using System.Collections.Generic;
    using System.Net;
    using Apache.Cassandra;
    using CassandraSharp.Config;
    using Thrift.Protocol;
    using Thrift.Transport;

    internal abstract class BaseTransportFactory : ITransportFactory
    {
        private readonly string _cqlver;

        private readonly string _password;

        private readonly string _user;

        protected BaseTransportFactory(TransportConfig config)
        {
            _cqlver = config.CqlVersion;
            _user = config.User;
            _password = config.Password;
        }

        public Cassandra.Client Create(IPAddress address)
        {
            TTransport transport = null;
            try
            {
                transport = CreateTransport(address);
                TProtocol protocol = new TBinaryProtocol(transport);
                Cassandra.Client client = new Cassandra.Client(protocol);

                if (null != _user && null != _password)
                {
                    AuthenticationRequest authenticationRequest = new AuthenticationRequest();
                    authenticationRequest.Credentials = new Dictionary<string, string>
                                                            {
                                                                {"username", _user},
                                                                {"password", _password}
                                                            };
                    client.login(authenticationRequest);
                }

                if (null != _cqlver)
                {
                    client.set_cql_version(_cqlver);
                }

                return client;
            }
            catch
            {
                CloseTransport(transport);
                throw;
            }
        }

        private static void CloseTransport(TTransport transport)
        {
            if (null != transport && transport.IsOpen)
            {
                try
                {
                    transport.Close();
                }
                catch
                {
                }
            }
        }

        protected abstract TTransport CreateTransport(IPAddress address);
    }
}