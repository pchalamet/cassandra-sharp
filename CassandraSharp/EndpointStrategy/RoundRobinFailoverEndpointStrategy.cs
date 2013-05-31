

namespace CassandraSharp.EndpointStrategy
{
    using System.Collections.Generic;
    using System.Net;
    using CassandraSharp.Extensibility;

    /// <summary>
    ///     Use RoundRobin stratgey on primary dc (must be set in config in EndPointsConfig)
    ///     until all nodes are unavailable then failover to all other dc's.
    /// </summary>
    internal sealed class RoundRobinFailoverEndpointStrategy : RoundRobinEndpointStrategy
    {
        private List<IPAddress> _failoverHealthyEndpoints;
        private List<IPAddress> _failoverBannedEndpoints;
        private bool _inFailover;
        private IEndpointSnitch _snitch;
        private bool _firstUpdateCalled;

        public RoundRobinFailoverEndpointStrategy(IEnumerable<IPAddress> endpoints, IEndpointSnitch snitch)
            : base(endpoints)
        {
            _failoverHealthyEndpoints = new List<IPAddress>();
            _failoverBannedEndpoints = new List<IPAddress>();
            _inFailover = false;
            _snitch = snitch;
            _firstUpdateCalled = false;
        }

        /// <summary>
        /// Swaps current nodes used by base class with the failover nodes managed by this class
        /// </summary>
        private void failover()
        {
            lock (_lock)
            {
                var tmp = _healthyEndpoints;
                _healthyEndpoints = _failoverHealthyEndpoints;
                _failoverHealthyEndpoints = tmp;

                tmp = _bannedEndpoints;
                _bannedEndpoints = _failoverBannedEndpoints;
                _failoverBannedEndpoints = tmp;

                _nextCandidate = 0;
                _inFailover = !_inFailover;
            }
        }

        public override void Ban(IPAddress endpoint)
        {
            if (_inFailover == _snitch.IsPrimaryDatacenter(endpoint))
            {
                //failover & primarydc or not failover and not primarydc then failover nodes affected
                lock (_lock)
                {
                    if (_failoverHealthyEndpoints.Remove(endpoint))
                    {
                        _failoverBannedEndpoints.Add(endpoint);
                    }
                }
            }
            else
            {
                base.Ban(endpoint);

                if (_healthyEndpoints.Count == 0)
                    failover();
            }

            
        }

        public override void Permit(IPAddress endpoint)
        {
            if (_inFailover == _snitch.IsPrimaryDatacenter(endpoint))
            {
                //failover & primarydc or not failover and not primarydc then failover nodes affected
                lock (_lock)
                {
                    if (_failoverBannedEndpoints.Remove(endpoint))
                    {
                        _failoverHealthyEndpoints.Add(endpoint);

                        //in failover and a new primary dc node has been added
                        if (_inFailover && _snitch.IsPrimaryDatacenter(endpoint))
                            failover(); //revert back to primary dc
                    }
                }
            }
            else
            {
                base.Permit(endpoint);
            }
        }

        public override void Update(NotificationKind kind, Peer peer)
        {
            //remove temporary list of all endpoitns created by base constructor
            if (!_firstUpdateCalled)
            {
                _healthyEndpoints = new List<IPAddress>();
                _firstUpdateCalled = true;
            }

            IPAddress endpoint = peer.RpcAddress;
            bool isPrimaryDc = _snitch.IsPrimaryDatacenter(endpoint);
            if (_inFailover == isPrimaryDc)
            {
                //failover & primarydc or not failover and not primarydc then failover nodes affected
                lock (_lock)
                {
                    switch (kind)
                    {
                        case NotificationKind.Add:
                        case NotificationKind.Update:
                            if (!_failoverHealthyEndpoints.Contains(endpoint)
                            && !_failoverBannedEndpoints.Contains(endpoint))
                            {
                                _failoverHealthyEndpoints.Add(endpoint);

                                //possibility new node added to revert existing failover
                                if (_inFailover && isPrimaryDc)
                                    failover();
                            }
                            break;
                        case NotificationKind.Remove:
                            if (_failoverHealthyEndpoints.Contains(endpoint))
                            {
                                _failoverHealthyEndpoints.Remove(endpoint);
                            }
                            else if (_failoverBannedEndpoints.Contains(endpoint))
                            {
                                _failoverBannedEndpoints.Remove(endpoint);
                            }
                            break;
                    }
                }
            }
            else
            {
                base.Update(kind, peer);

                if (kind == NotificationKind.Remove && _healthyEndpoints.Count == 0)
                    failover();
            }
        }

    }
}
