namespace CassandraSharp.Factory
{
    using CassandraSharp.Recovery;

    internal static class RecoveryServiceExtensions
    {
        public static IRecoveryService Create()
        {
            return new DefaultRecovery();
        }
    }
}