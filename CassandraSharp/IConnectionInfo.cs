namespace CassandraSharp
{
    public interface IConnectionInfo
    {
        string KeySpace { get; }

        string Login { get; }

        string Password { get; }
    }
}