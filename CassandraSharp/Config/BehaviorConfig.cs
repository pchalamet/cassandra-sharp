namespace CassandraSharp.Config
{
    using System.Xml.Serialization;
    using Apache.Cassandra;

    public class BehaviorConfig : IBehaviorConfig
    {
        public BehaviorConfig()
        {
            ReadConsistencyLevel = ConsistencyLevel.QUORUM;
            WriteConsistencyLevel = ConsistencyLevel.QUORUM;
        }

        [XmlAttribute("keyspace")]
        public string KeySpace { get; set; }

        [XmlAttribute("readCL")]
        public ConsistencyLevel ReadConsistencyLevel { get; set; }

        [XmlAttribute("ttl")]
        public int TTL { get; set; }

        [XmlAttribute("writeCL")]
        public ConsistencyLevel WriteConsistencyLevel { get; set; }

        [XmlAttribute("maxRetries")]
        public int MaxRetries { get; set; }

        [XmlAttribute("password")]
        public string Password { get; set; }

        [XmlAttribute("retryOnNotFound")]
        public bool RetryOnNotFound { get; set; }

        [XmlAttribute("retryOnTimeout")]
        public bool RetryOnTimeout { get; set; }

        [XmlAttribute("retryOnUnavailable")]
        public bool RetryOnUnavailable { get; set; }

        [XmlAttribute("user")]
        public string User { get; set; }
    }
}