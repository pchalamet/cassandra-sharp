namespace CassandraSharp.Factory
{
    using System;
    using System.Collections.Generic;
    using CassandraSharp.Config;
    using CassandraSharp.EndpointStrategy;

    internal static class EndpointsConfigExtensions
    {
        public static IEndpointStrategy Create(this EndpointsConfig @this, string customType, IEnumerable<Endpoint> endpoints)
        {
            switch (@this.Strategy)
            {
                case EndpointStrategy.Custom:
                    if (null == customType)
                    {
                        throw new ArgumentNullException("EndpointsConfig.StrategyType");
                    }

                    Type strategyType = Type.GetType(customType);
                    if (null == strategyType || !typeof(IEndpointStrategy).IsAssignableFrom(strategyType))
                    {
                        string invalidTypeMsg = string.Format("'{0}' is not a valid type", customType);
                        throw new ArgumentException(invalidTypeMsg);
                    }

                    return (IEndpointStrategy) Activator.CreateInstance(strategyType, endpoints);

                case EndpointStrategy.Random:
                    return new RandomEndpointStrategy(endpoints);

                case EndpointStrategy.Nearest:
                    return new NearestEndpointStrategy(endpoints);
            }

            string msg = string.Format("Unknown strategy '{0}'", @this);
            throw new ArgumentException(msg);
        }
    }
}