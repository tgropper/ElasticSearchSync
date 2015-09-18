using System;
using System.Configuration;
using System.Runtime.Serialization;

namespace ElasticSearchSync.Helpers
{
    public class ConfigSection : ConfigurationSection
    {
        [IgnoreDataMember()]
        public static ConfigSection Default
        {
            get { return (ConfigSection)ConfigurationManager.GetSection("ElasticSearchSync"); }
        }

        [IgnoreDataMember]
        [ConfigurationProperty("index")]
        public IndexConfigurationElement Index
        {
            get { return (IndexConfigurationElement)this["index"]; }
            set { this["index"] = value; }
        }

        [IgnoreDataMember]
        [ConfigurationProperty("concurrency")]
        public ConcurrencyConfigurationElement Concurrency
        {
            get { return (ConcurrencyConfigurationElement)this["concurrency"]; }
            set { this["concurrency"] = value; }
        }

        [DataContract]
        public class IndexConfigurationElement : ConfigurationElement
        {
            [DataMember]
            [ConfigurationProperty("name", IsRequired = true)]
            public string Name
            {
                get { return (string)this["name"]; }
                set { this["name"] = value; }
            }
        }

        [DataContract]
        public class ConcurrencyConfigurationElement : ConfigurationElement
        {
            [DataMember]
            [ConfigurationProperty("duration", IsRequired = false)]
            public TimeSpan Duration
            {
                get { return (TimeSpan)this["duration"]; }
                set { this["duration"] = value; }
            }
        }
    }
}