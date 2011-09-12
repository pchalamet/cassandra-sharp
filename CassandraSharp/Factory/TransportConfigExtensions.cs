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
namespace CassandraSharp.Factory
{
    using System;
    using CassandraSharp.Config;
    using CassandraSharp.Transport;

    internal static class TransportConfigExtensions
    {
        public static ITransportFactory Create(this TransportConfig @this)
        {
            switch (@this.Type)
            {
                case TransportConfig.TransportType.Framed:
                    return new FramedTransportFactory(@this);

                case TransportConfig.TransportType.Buffered:
                    return new BufferedTransportFactory(@this);
            }

            string msg = string.Format("Unknown transport type '{0}'", @this.Type);
            throw new ArgumentException(msg);
        }
    }
}