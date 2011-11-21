namespace CassandraSharp
{
    public class ConnectionInfo : IConnectionInfo
    {
        public ConnectionInfo(string keySpace)
            : this(keySpace, null, null)
        {
        }

        public ConnectionInfo(string keySpace, string login, string password)
        {
            KeySpace = keySpace;
            Login = login;
            Password = password;
        }

        public string KeySpace { get; private set; }

        public string Login { get; private set; }

        public string Password { get; private set; }
    }
}