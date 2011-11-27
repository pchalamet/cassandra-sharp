namespace CassandraSharp.Recovery
{
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;
    using System.Timers;
    using CassandraSharp.EndpointStrategy;
    using CassandraSharp.Transport;
    using CassandraSharp.Utils;
    using log4net;
    using Thrift.Transport;

    internal class DefaultRecovery : IRecoveryService
    {
        private readonly Timer _timer;

        private readonly List<RecoveryItem> _toRecover;

        private ILog _log = LogManager.GetLogger(typeof(DefaultRecovery));

        public DefaultRecovery()
        {
            _toRecover = new List<RecoveryItem>();
            _timer = new Timer(60*1000);
            _timer.Elapsed += TryRecover;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Recover(Endpoint endpoint, IEndpointStrategy endpointStrategy, ITransportFactory transportFactory)
        {
            _log.Info(string.Format("marking {0} for recovery", endpoint.Address));

            RecoveryItem recoveryItem = new RecoveryItem(endpoint, transportFactory, endpointStrategy);
            _toRecover.Add(recoveryItem);

            _timer.Enabled = true;
        }

        public void Dispose()
        {
            _log.Info("Recovery service is shutting down");

            _timer.Enabled = false;
            _timer.SafeDispose();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private void TryRecover(object sender, ElapsedEventArgs e)
        {
            _log.Info("Recovery service is trying to recover connections");

            List<RecoveryItem> recoveredItems = new List<RecoveryItem>();
            foreach (RecoveryItem recoveryItem in _toRecover)
            {
                bool ok = false;
                TTransport transport = null;
                try
                {
                    transport = recoveryItem.TransportFactory.Create(recoveryItem.Endpoint.Address);
                    transport.Open();
                    ok = true;
                }
// ReSharper disable EmptyGeneralCatchClause
                catch
// ReSharper restore EmptyGeneralCatchClause
                {
                }
                finally
                {
                    if (null != transport && transport.IsOpen)
                    {
                        transport.Close();
                    }
                }

                if (ok)
                {
                    recoveryItem.EndpointStrategy.Permit(recoveryItem.Endpoint);
                    recoveredItems.Add(recoveryItem);
                }
            }

            foreach (RecoveryItem recoveryItem in recoveredItems)
            {
                _log.Info(string.Format("{0} is recovered", recoveryItem.Endpoint.Address));

                _toRecover.Remove(recoveryItem);
            }

            if (0 == _toRecover.Count)
            {
                _timer.Enabled = false;
            }
        }

        private class RecoveryItem
        {
            public RecoveryItem(Endpoint endpoint, ITransportFactory transportFactory, IEndpointStrategy endpointStrategy)
            {
                Endpoint = endpoint;
                TransportFactory = transportFactory;
                EndpointStrategy = endpointStrategy;
            }

            public Endpoint Endpoint { get; private set; }

            public ITransportFactory TransportFactory { get; private set; }

            public IEndpointStrategy EndpointStrategy { get; private set; }
        }
    }
}