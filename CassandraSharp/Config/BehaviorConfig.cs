// cassandra-sharp - a .NET client for Apache Cassandra
// Copyright (c) 2011-2012 Pierre Chalamet
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

namespace CassandraSharp.Config
{
    using System.Xml.Serialization;
    using CassandraSharp.Extensibility;

    public class BehaviorConfig : IBehaviorConfig
    {
        //[XmlAttribute("keyspace")]
        //public string KeySpace { get; set; }

        //[XmlAttribute("readCL")]
        //public ConsistencyLevel? DefaultReadCL { get; set; }

        //[XmlAttribute("ttl")]
        //public int? DefaultTTL { get; set; }

        //[XmlAttribute("writeCL")]
        //public ConsistencyLevel? DefaultWriteCL { get; set; }

        //[XmlAttribute("maxRetries")]
        //public int? MaxRetries { get; set; }

        //[XmlAttribute("retryOnNotFound")]
        //public bool? RetryOnNotFound { get; set; }

        //[XmlAttribute("retryOnTimeout")]
        //public bool? RetryOnTimeout { get; set; }

        //[XmlAttribute("retryOnUnavailable")]
        //public bool? RetryOnUnavailable { get; set; }

        //[XmlAttribute("sleepBeforeRetry")]
        //public int? SleepBeforeRetry { get; set; }
    }
}