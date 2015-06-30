using System.Data.SqlClient;

namespace ElasticSearchSync
{
    public class SyncDeleteConfiguration
    {
        /// <summary>
        /// Sql exec must return a datareader containing mandatorily a column with document _id
        /// </summary>
        public SqlCommand SqlCommand { get; set; }

        /// <summary>
        /// Add to the WHERE clause the condition that objects to consider in the process have been created or updated after the last synchronization
        /// If this property has value, process will expect the SqlCommand to have a WHERE clause
        /// </summary>
        public string[] ColumnsToCompareWithLastSyncDate { get; set; }
    }
}