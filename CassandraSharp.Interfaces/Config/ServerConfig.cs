

namespace CassandraSharp.Config
{
    using System.Xml.Serialization;

    public class ServerConfig
    {

        public ServerConfig()
        {
            //leave default value empty for better error messages
            DataCentre = "";
            Rack = "";
        }

        [XmlTextAttribute]
        public string Server { get; set; }

        [XmlAttribute("dc")]
        public string DataCentre { get; set; }

        [XmlAttribute("rack")]
        public string Rack { get; set; }
    }
}
