namespace CassandraSharp
{
    using System;
    using CassandraSharp.EndpointStrategy;
    using CassandraSharp.Transport;

    internal interface IRecoveryService : IDisposable
    {
        void Recover(Endpoint connection, IEndpointStrategy endpointsManager, ITransportFactory transportFactory);
    }
}