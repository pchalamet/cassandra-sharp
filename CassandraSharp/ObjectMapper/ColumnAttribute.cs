namespace CassandraSharp.ObjectMapper
{
    using System;

    public enum DataType
    {
        Auto,

        // US-ASCII character string
        Ascii,

        // 64-bit signed long
        BigInt,

        // Arbitrary bytes (no validation), expressed as hexadecimal
        Blob,

        // true or false
        Boolean,

        // Distributed counter value (64-bit long)
        Counter,

        // Variable-precision decimal
        Decimal,

        // 64-bit IEEE-754 floating point
        Double,

        // 32-bit IEEE-754 floating point
        Float,

        // 32-bit signed integer
        Int,

        // UTF-8 encoded string
        Text,

        // Date plus time, encoded as 8 bytes since epoch
        Timestamp,

        // Type 1 or type 4 UUID
        Uuid,

        // UTF-8 encoded string
        Varchar,

        // Arbitrary-precision integer
        Varint,
    }

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