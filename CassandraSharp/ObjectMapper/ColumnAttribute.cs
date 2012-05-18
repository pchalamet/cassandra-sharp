namespace CassandraSharp.ObjectMapper
{
    using System;

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class ColumnAttribute : Attribute
    {
        public ColumnAttribute()
        {
            DataType = DataType.Auto;
        }

        public string Name { get; set; }

        public DataType DataType { get; set; }
    }
}