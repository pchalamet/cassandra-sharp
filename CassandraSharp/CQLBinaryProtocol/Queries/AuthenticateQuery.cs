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

namespace CassandraSharp.CQLBinaryProtocol.Queries
{
    using System.Collections.Generic;
    using System.IO;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils.Stream;

    internal sealed class AuthenticateQuery : Query<bool>
    {
        private readonly string _password;

        private readonly string _user;

        public AuthenticateQuery(IConnection connection, string user, string password)
                : base(connection)
        {
            _user = user;
            _password = password;
        }

        protected override IEnumerable<bool> ReadFrame(IFrameReader frameReader)
        {
            bool res = frameReader.MessageOpcode == MessageOpcodes.Ready;
            yield return res;
        }

        protected override void WriteFrame(IFrameWriter fw)
        {
            Stream stream = fw.WriteOnlyStream;
            Dictionary<string, string> authParams = new Dictionary<string, string>
                {
                        {"username", _user},
                        {"password", _password}
                };
            stream.WriteStringMap(authParams);
            fw.SetMessageType(MessageOpcodes.Credentials);
        }

        protected override InstrumentationToken CreateToken()
        {
            InstrumentationToken token = InstrumentationToken.Create(RequestType.Authenticate, ExecutionFlags.None);
            return token;
        }
    }
}