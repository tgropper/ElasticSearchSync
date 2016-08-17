using Elasticsearch.Net.Connection;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace ElasticSearchSync
{
    public class SyncConfiguration
    {
        /// <summary>
        /// SqlServer connection
        /// </summary>
        public SqlConnection SqlConnection { get; set; }

        /// <summary>
        /// First column of sql script will be used as document _id
        /// </summary>
        public SqlCommand SqlCommand { get; set; }

        public IEnumerable<SyncArrayConfiguration> ArraysConfiguration { get; set; }

        public IEnumerable<SyncObjectConfiguration> ObjectsConfiguration { get; set; }

        public SyncDeleteConfiguration DeleteConfiguration { get; set; }

        /// <summary>
        /// Add to the WHERE clause the condition that objects to consider in the process have been created or updated after the last synchronization
        /// If this property has value, process will expect the SqlCommand to have a WHERE clause
        /// </summary>
        public string[] ColumnsToCompareWithLastSyncDate { get; set; }

        /// <summary>
        /// Sql columns that contains xml data
        /// </summary>
        public string[] XmlFields { get; set; }

        public ConnectionConfiguration ElasticSearchConfiguration { get; set; }

        /// <summary>
        /// Max number of documents in a single bulk request
        /// </summary>
        public int BulkSize { get; set; }

        /// <summary>
        /// Configures pagination on Sql queries.
        /// </summary>
        public int? PageSize { get; set; }

        /// <summary>
        /// Elasticsearch index
        /// </summary>
        public string _Index { get; set; }

        /// <summary>
        /// Elasticsearch type
        /// </summary>
        public string _Type { get; set; }

        public SyncConfiguration()
        {
            BulkSize = 1000;
            ArraysConfiguration = new List<SyncArrayConfiguration>();
            ObjectsConfiguration = new List<SyncObjectConfiguration>();
        }
    }
}