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

namespace CassandraSharp.CQLPropertyBag
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;

    public sealed class PropertyBag
    {
        private readonly Dictionary<string, object> _map = new Dictionary<string, object>();

        public object this[string name]
        {
            get
            {
                string lowName = name.ToLower(CultureInfo.InvariantCulture).Replace("_", "");
                return _map[lowName];
            }

            set
            {
                string lowName = name.ToLower(CultureInfo.InvariantCulture).Replace("_", "");
                _map[lowName] = value;
            }
        }

        public string[] Keys
        {
            get { return _map.Keys.ToArray(); }
        }
    }
}