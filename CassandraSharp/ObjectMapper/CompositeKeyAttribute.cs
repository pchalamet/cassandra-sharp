namespace CassandraSharp.ObjectMapper
{
    using System;

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class CompositeKeyAttribute : KeyAttribute
    {
        public int Index { get; set; }
    }
}