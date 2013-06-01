

namespace CassandraSharp.Snitch
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using CassandraSharp.Extensibility;

    /// <summary>
    ///     Takes all network topology information from the discovery servive's ISnitch.Update event notification
    /// </summary>
    internal sealed class DiscoverySnitch : IEndpointSnitch
    {
        private readonly Dictionary<IPAddress, Peer> _networkTopology;
        private readonly string _primaryDatacenter;
        private readonly object _lock = new object();

        public DiscoverySnitch(string primaryDatacenter)
        {
            _networkTopology = new Dictionary<IPAddress, Peer>();
            _primaryDatacenter = primaryDatacenter;
        }


        private Peer GetTopology(IPAddress endpoint)
        {
            try
            {
                return _networkTopology[endpoint];
            }
            catch
            {
                throw new SnitchNotReadyException(endpoint.ToString());
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="endpoint"></param>
        /// <exception cref="SnitchNotReadyException">
        /// When endpoint not initialised in the snitch's internal dictionary
        /// This is only usually thrown by SystemPeersDiscoveryService depending on Endpoint Strategy
        /// </exception>
        /// <returns></returns>
        public string GetRack(IPAddress endpoint)
        {
            return GetTopology(endpoint).Rack;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="endpoint"></param>
        /// <exception cref="SnitchNotReadyException">
        /// When endpoint not initialised in the snitch's internal dictionary
        /// This is only usually thrown by SystemPeersDiscoveryService depending on Endpoint Strategy
        /// </exception>
        /// <returns></returns>
        public string GetDatacenter(IPAddress endpoint)
        {
            return GetTopology(endpoint).Datacenter;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="endpoint"></param>
        /// <returns></returns>
        public bool IsPrimaryDatacenter(IPAddress endpoint)
        {
            return GetDatacenter(endpoint).Equals(_primaryDatacenter, StringComparison.InvariantCulture);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <param name="unsortedAddress"></param>
        /// <exception cref="SnitchNotReadyException">
        /// When endpoint not initialised in the snitch's internal dictionary
        /// This is only usually thrown by SystemPeersDiscoveryService depending on Endpoint Strategy
        /// </exception>
        /// <returns></returns>
        public List<IPAddress> GetSortedListByProximity(IPAddress address, IEnumerable<IPAddress> unsortedAddress)
        {
            var addressTopology = GetTopology(address);

            return unsortedAddress
                .OrderBy(ip => addressTopology.GetProximity(GetTopology(ip)))
                .ToList();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <param name="a1"></param>
        /// <param name="a2"></param>
        /// <exception cref="SnitchNotReadyException">
        /// When endpoint not initialised in the snitch's internal dictionary
        /// This is only usually thrown by SystemPeersDiscoveryService depending on Endpoint Strategy
        /// </exception>
        /// <returns></returns>
        public int CompareEndpoints(IPAddress address, IPAddress a1, IPAddress a2)
        {
            var addressTopology = GetTopology(address);
            int proximityA1 = addressTopology.GetProximity(GetTopology(a1));
            int proximityA2 = addressTopology.GetProximity(GetTopology(a2));

            if (proximityA1 == proximityA2)
                return 0; //address is neither closer to a1 or a2
            else if (proximityA1 < proximityA2)
                return -1; //address is closer to a1
            else
                return 1; //address is closer to a2
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="kind"></param>
        /// <param name="peer"></param>
        public void Update(NotificationKind kind, Peer peer)
        {
            lock (_lock)
            {
                IPAddress endpoint = peer.RpcAddress;
                switch (kind)
                {
                    case NotificationKind.Add:
                    case NotificationKind.Update:
                        if (_networkTopology.ContainsKey(endpoint))
                            _networkTopology.Remove(endpoint);
                        _networkTopology.Add(endpoint, peer);
                        break;
                    case NotificationKind.Remove:
                        if (_networkTopology.ContainsKey(endpoint))
                            _networkTopology.Remove(endpoint);
                        break;
                }
            }
        }

        [Serializable]
        public class SnitchNotReadyException : Exception
        {
            public SnitchNotReadyException() : base() { }
            public SnitchNotReadyException(string message) : base(message) { }
            public SnitchNotReadyException(string message, System.Exception inner) : base(message, inner) { }

            protected SnitchNotReadyException(System.Runtime.Serialization.SerializationInfo info,
                System.Runtime.Serialization.StreamingContext context) { }
        }
    }
}
