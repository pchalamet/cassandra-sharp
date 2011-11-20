namespace CassandraSharp.Config
{
    using System.Xml.Serialization;
    using Apache.Cassandra;

    public class BehaviorConfig
    {
        [XmlAttribute("defaultKeyspace")]
        public string DefaultKeyspace;

        [XmlAttribute("defaultReadConsistencyLevel")]
        public ConsistencyLevel DefaultReadConsistencyLevel = ConsistencyLevel.QUORUM;

        [XmlAttribute("defaultTTL")]
        public int DefaultTTL;

        [XmlAttribute("defaultWriteConsistencyLevel")]
        public ConsistencyLevel DefaultWriteConsistencyLevel = ConsistencyLevel.QUORUM;

        [XmlAttribute("maxRetries")]
        public int MaxRetries;

        [XmlAttribute("password")]
        public string Password;

        [XmlAttribute("poolSize")]
        public int PoolSize;

        [XmlAttribute("retryOnNotFound")]
        public bool RetryOnNotFound;

        [XmlAttribute("retryOnTimeout")]
        public bool RetryOnTimeout;

        [XmlAttribute("retryOnUnavailable")]
        public bool RetryOnUnavailable;

        [XmlAttribute("user")]
        public string User;
    }
}