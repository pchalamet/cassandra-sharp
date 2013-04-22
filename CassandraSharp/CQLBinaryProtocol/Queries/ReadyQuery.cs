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
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils.Stream;

    internal class ReadyQuery : Query<bool>
    {
        private readonly string _cqlVersion;

        public ReadyQuery(IConnection connection, string cqlVersion)
                : base(connection)
        {
            _cqlVersion = cqlVersion;
        }

        protected override IEnumerable<bool> ReadFrame(IFrameReader frameReader)
        {
            bool mustAuthenticate = frameReader.MessageOpcode == MessageOpcodes.Authenticate;
            yield return mustAuthenticate;
        }

        protected override void WriteFrame(IFrameWriter fw)
        {
            Dictionary<string, string> options = new Dictionary<string, string>
                {
                        {"CQL_VERSION", _cqlVersion}
                };
            fw.WriteOnlyStream.WriteStringMap(options);
            fw.SetMessageType(MessageOpcodes.Startup);
        }

        protected override InstrumentationToken CreateToken()
        {
            InstrumentationToken token = InstrumentationToken.Create(RequestType.Ready, ExecutionFlags.None);
            return token;
        }
    }
}