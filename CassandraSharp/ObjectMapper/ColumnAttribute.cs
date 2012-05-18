namespace CassandraSharp.ObjectMapper
{
    using System;

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class ColumnAttribute : Attribute
    {
        public ColumnAttribute()
        {
            CqlType = CqlType.Auto;
        }

        public string Name { get; set; }

        public CqlType CqlType { get; set; }
    }
}