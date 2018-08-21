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
using CassandraSharp.Extensibility;
using CassandraSharp.Utils.Stream;

namespace CassandraSharp.CQLBinaryProtocol.Queries
{
    internal sealed class AuthenticateQuery : Query<bool>
    {
        private readonly string _password;

        private readonly string _user;

        public AuthenticateQuery(IConnection connection, ConsistencyLevel consistencyLevel, ExecutionFlags executionFlags, string user, string password)
            : base(connection, consistencyLevel, executionFlags)
        {
            _user = user;
            _password = password;
        }

        protected override IEnumerable<bool> ReadFrame(IFrameReader frameReader)
        {
            var res = frameReader.MessageOpcode == MessageOpcodes.Ready;
            yield return res;
        }

        protected override void WriteFrame(IFrameWriter fw)
        {
            var stream = fw.WriteOnlyStream;
            var authParams = new Dictionary<string, string>
                             {
                                 {"username", _user},
                                 {"password", _password}
                             };
            stream.WriteStringMap(authParams);
            fw.SetMessageType(MessageOpcodes.Credentials);
        }

        protected override InstrumentationToken CreateInstrumentationToken()
        {
            var token = InstrumentationToken.Create(RequestType.Authenticate, ExecutionFlags.None);
            return token;
        }
    }
}