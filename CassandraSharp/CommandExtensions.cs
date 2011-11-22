namespace CassandraSharp
{
    using System;
    using Apache.Cassandra;

    public static class CommandExtensions
    {
        public static void Execute(this ICluster @this, Action<Cassandra.Client> action)
        {
            @this.Execute(null, action);
        }

        public static void Execute(this ICluster @this, IBehaviorConfig behaviorConfig, Action<Cassandra.Client> action)
        {
            Func<Cassandra.Client, object> func = client =>
                                                      {
                                                          action(client);
                                                          return null;
                                                      };
            @this.ExecuteCommand(behaviorConfig, func);
        }
    }
}