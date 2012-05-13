namespace CassandraSharp.ObjectMapper
{
    using System;

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class KeyAttribute : ColumnAttribute
    {
    }
}