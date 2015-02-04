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

        /// <summary>
        /// First column of sql script must be the same column used for document _id
        /// </summary>
        public IEnumerable<SqlCommand> ArraySqlCommands { get; set; }

        public ConnectionConfiguration ElasticSearchConfiguration { get; set; }

        /// <summary>
        /// Max number of documents in a single bulk request
        /// </summary>
        public int BulkSize { get; set; }

        public string _Index { get; set; }

        public string _Type { get; set; }
    }
}
