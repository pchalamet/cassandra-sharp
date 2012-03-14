namespace CassandraSharp.Factory
{
    using System;
    using CassandraSharp.Config;
    using CassandraSharp.Snitch;

    internal static class SnitchTypeExtensions
    {
        public static ISnitch Create(this SnitchType @this, string customType)
        {
            switch (@this)
            {
                case SnitchType.Custom:
                    if (null == customType)
                    {
                        throw new ArgumentNullException("EndpointsConfig.SnitchType");
                    }

                    Type snitchType = Type.GetType(customType);
                    if (null == snitchType || !typeof(ISnitch).IsAssignableFrom(snitchType))
                    {
                        string invalidTypeMsg = string.Format("'{0}' is not a valid type", customType);
                        throw new ArgumentException(invalidTypeMsg);
                    }

                    return (ISnitch) Activator.CreateInstance(snitchType);

                case SnitchType.Simple:
                    return new SimpleSnitch();

                case SnitchType.RackInferring:
                    return new RackInferringSnitch();
            }

            string msg = string.Format("Unknown snitch type '{0}'", @this);
            throw new ArgumentException(msg);
        }
    }
}