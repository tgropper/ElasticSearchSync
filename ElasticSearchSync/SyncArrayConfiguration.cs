namespace ElasticSearchSync
{
    /// <summary>
    /// It allows to insert an array into de primary object
    /// </summary>
    public class SyncArrayConfiguration : SyncObjectConfiguration
    {
        /// <summary>
        /// Sql columns that contains xml data
        /// </summary>
        public string[] XmlFields { get; set; }
    }
}