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

        public SyncDeleteConfiguration DeleteConfiguration { get; set; }

        /// <summary>
        /// Add to the WHERE clause an IN condition for ids of the objects in arrays.
        /// If this property has value, process will expect the WHERE clause of ArraySqlCommands to be with format (to use String.Replace)
        /// Property ParentIdColumn in ArrayConfiguration must have a value
        /// </summary>
        public bool FilterArrayByParentsIds { get; set; }

        /// <summary>
        /// Add to the WHERE clause the condition that objects to consider in the process have been created or updated after the last synchronization
        /// If this property has value, process will expect the SqlCommand to have a WHERE clause:
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
        }
    }

    public class SyncArrayConfiguration
    {
        /// <summary>
        /// First column of sql script must be the same column used for document _id
        /// </summary>
        public SqlCommand SqlCommand { get; set; }

        /// <summary>
        /// Relative position where array is gonna to be added in the serialized object
        /// NOTE: selected relative position must be an object, not directly an attribute
        /// </summary>
        /// <example>
        /// serialized object:
        ///     note: {
        ///         title
        ///         body
        ///     }
        ///
        /// attributeName:
        ///     note.tags
        ///
        /// final serialized object:
        ///     note: {
        ///         title
        ///         body
        ///         tags: [
        ///             {/array object to be inserted/}
        ///         ]
        ///     }
        /// </example>
        public string AttributeName { get; set; }

        public string ParentIdColumn { get; set; }

        /// <summary>
        /// Sql columns that contains xml data
        /// </summary>
        public string[] XmlFields { get; set; }

        /// <summary>
        /// If it has value, the array will be inserted within another one, matching the value of the second propery with the array element taken by this key
        /// </summary>
        public string InsertIntoArrayComparerKey { get; set; }
    }

    public class SyncDeleteConfiguration
    {
        /// <summary>
        /// Sql exec must return a datareader containing mandatorily a column with document _id
        /// </summary>
        public SqlCommand SqlCommand { get; set; }

        /// <summary>
        /// Add to the WHERE clause the condition that objects to consider in the process have been created or updated after the last synchronization
        /// If this property has value, process will expect the SqlCommand to have a WHERE clause:
        /// </summary>
        public string[] ColumnsToCompareWithLastSyncDate { get; set; }
    }
}