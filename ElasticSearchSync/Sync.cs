using Bardock.Utils.Extensions;
using Elasticsearch.Net;
using ElasticSearchSync.Helpers;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;

namespace ElasticSearchSync
{
    public class Sync
    {
        public ElasticsearchClient client { get; set; }

        public log4net.ILog log { get; set; }

        private Stopwatch stopwatch { get; set; }

        private SyncConfiguration _config;
        private const string LogIndex = "sqlserver_es_sync";
        private string LogType = "log";
        private string BulkLogType = "bulk_log";
        private string LockType = "lock";
        private string LastLogType = "last_log";
        private const string LastLogID = "1";

        public Sync(SyncConfiguration config)
        {
            _config = config;
            log4net.Config.BasicConfigurator.Configure();
            log = log4net.LogManager.GetLogger("SQLSERVER-ES Sync");
            stopwatch = new Stopwatch();

            LogType = String.Format("{0}_{1}_{2}", LogType, _config._Index, _config._Type);
            BulkLogType = String.Format("{0}_{1}_{2}", BulkLogType, _config._Index, _config._Type);
            LockType = String.Format("{0}_{1}_{2}", LockType, _config._Index, _config._Type);
            LastLogType = String.Format("{0}_{1}_{2}", LastLogType, _config._Index, _config._Type);
        }

        public SyncResponse Exec()
        {
            var startedOn = DateTime.UtcNow;
            log.Info("process started at " + startedOn.NormalizedFormat());
            client = new ElasticsearchClient(_config.ElasticSearchConfiguration);

            using (var _lock = new SyncLock(client, LogIndex, LockType))
            {
                DateTime? lastSyncDate = ConfigureIncrementalProcess(_config.SqlCommand, _config.ColumnsToCompareWithLastSyncDate);

                var data = GetSerializedObject();
                log.Info(String.Format("{0} objects have been serialized.", data.Count()));

                var syncResponse = new SyncResponse(startedOn);

                syncResponse = IndexProcess(data, syncResponse);

                if (_config.DeleteConfiguration != null)
                {
                    _config.SqlConnection.Open();
                    Dictionary<object, Dictionary<string, object>> deleteData = null;

                    if (lastSyncDate != null)
                        ConfigureIncrementalProcess(_config.DeleteConfiguration.SqlCommand, _config.DeleteConfiguration.ColumnsToCompareWithLastSyncDate, lastSyncDate);

                    using (SqlDataReader rdr = _config.DeleteConfiguration.SqlCommand.ExecuteReader())
                    {
                        deleteData = rdr.Serialize();
                    }
                    _config.SqlConnection.Close();

                    syncResponse = DeleteProcess(deleteData, syncResponse);
                }

                syncResponse = Log(syncResponse);

                log.Info(String.Format("process duration: {0}ms", Math.Truncate((syncResponse.EndedOn - syncResponse.StartedOn).TotalMilliseconds)));

                return syncResponse;
            }
        }

        /// <summary>
        /// Build and add to the sql where clause the lastSyncDate condition, taken from elasticsearch sync log
        /// </summary>
        private DateTime? ConfigureIncrementalProcess(SqlCommand sqlCommand, string[] columnsToCompareWithLastSyncDate, DateTime? lastSyncDate = null)
        {
            if (_config.ColumnsToCompareWithLastSyncDate != null)
            {
                if (lastSyncDate == null)
                {
                    var lastSyncResponse = GetLastSync();
                    if (lastSyncResponse == null || lastSyncResponse.Response == null || (bool)lastSyncResponse.Response["found"] == false)
                    {
                        return null;
                    }

                    lastSyncDate = DateTime.Parse(lastSyncResponse.Response["_source"]["date"]).ToUniversalTime();
                }

                var conditionBuilder = new StringBuilder("(");
                foreach (var col in columnsToCompareWithLastSyncDate)
                    conditionBuilder
                        .Append(col)
                        .Append(" >= '")
                        .Append(lastSyncDate.Value.NormalizedFormat())
                        .Append("' OR ");
                conditionBuilder.RemoveLastCharacters(4).Append(")");

                sqlCommand.CommandText = AddSqlCondition(sqlCommand.CommandText, conditionBuilder.ToString());
            }

            return lastSyncDate;
        }

        private ElasticsearchResponse<DynamicDictionary> GetLastSync()
        {
            stopwatch.Start();
            ElasticsearchResponse<DynamicDictionary> lastSyncResponse = null;
            try
            {
                lastSyncResponse = client.Get(LogIndex, LastLogType, LastLogID);
            }
            catch (WebException)
            { }
            
            stopwatch.Stop();
            log.Debug(String.Format("last sync search duration: {0}ms", stopwatch.ElapsedMilliseconds));
            stopwatch.Reset();

            return lastSyncResponse;
        }

        private BulkResponse BulkIndexProcess(Dictionary<object, Dictionary<string, object>> data)
        {
            return BulkProcess(data, ElasticsearchHelpers.GetPartialIndexBulk);
        }

        private SyncResponse IndexProcess(Dictionary<object, Dictionary<string, object>> data, SyncResponse syncResponse)
        {
            var c = 0;
            while (c < data.Count())
            {
                var partialData = data.Skip(c).Take(_config.BulkSize).ToDictionary(x => x.Key, x => x.Value);

                var bulkResponse = BulkIndexProcess(partialData);

                syncResponse.BulkResponses.Add(bulkResponse);
                syncResponse.IndexedDocuments += bulkResponse.AffectedDocuments;
                syncResponse.Success = syncResponse.Success && bulkResponse.Success;

                log.Info(String.Format("bulk duration: {0}ms. so far {1} documents have been indexed successfully.", bulkResponse.Duration, syncResponse.IndexedDocuments));
                c += _config.BulkSize;
            }

            return syncResponse;
        }

        private BulkResponse BulkDeleteProcess(Dictionary<object, Dictionary<string, object>> data)
        {
            return BulkProcess(data, ElasticsearchHelpers.GetPartialDeleteBulk);
        }

        private SyncResponse DeleteProcess(Dictionary<object, Dictionary<string, object>> data, SyncResponse syncResponse)
        {
            var d = 0;
            while (d < data.Count())
            {
                var partialData = data.Skip(d).Take(_config.BulkSize).ToDictionary(x => x.Key, x => x.Value);

                var bulkResponse = BulkDeleteProcess(partialData);

                syncResponse.BulkResponses.Add(bulkResponse);
                syncResponse.DeletedDocuments += bulkResponse.AffectedDocuments;
                syncResponse.Success = syncResponse.Success && bulkResponse.Success;

                log.Info(String.Format("bulk duration: {0}ms. so far {1} documents have been deleted successfully.", bulkResponse.Duration, syncResponse.DeletedDocuments));
                d += _config.BulkSize;
            }

            return syncResponse;
        }

        private BulkResponse BulkProcess(
            Dictionary<object, Dictionary<string, object>> data,
            Func<string, string, object, Dictionary<string, object>, string> getPartialBulk)
        {
            stopwatch.Start();
            string partialbulk = string.Empty;
            var bulkStartedOn = DateTime.UtcNow;

            //build bulk data
            foreach (var bulkData in data)
                partialbulk = partialbulk + getPartialBulk(_config._Index, _config._Type, bulkData.Key, bulkData.Value);

            var response = client.Bulk(partialbulk);
            stopwatch.Stop();

            var bulkResponse = new BulkResponse
            {
                Success = response.Success,
                HttpStatusCode = response.HttpStatusCode,
                AffectedDocuments = response.Response["items"].HasValue ? ((object[])response.Response["items"].Value).Length : 0,
                ESexception = response.OriginalException,
                StartedOn = bulkStartedOn,
                Duration = stopwatch.ElapsedMilliseconds
            };
            LogBulk(bulkResponse);
            stopwatch.Reset();

            return bulkResponse;
        }

        /// <summary>
        /// Log in {logIndex}/{logBulkType} the bulk serializedNewObject and metrics
        /// </summary>
        private void LogBulk(BulkResponse bulkResponse)
        {
            client.Index(LogIndex, BulkLogType, new
            {
                success = bulkResponse.Success,
                httpStatusCode = bulkResponse.HttpStatusCode,
                documentsIndexed = bulkResponse.AffectedDocuments,
                startedOn = bulkResponse.StartedOn,
                duration = bulkResponse.Duration + "ms",
                exception = bulkResponse.ESexception != null ? ((Exception)bulkResponse.ESexception).Message : null
            });
        }

        /// <summary>
        /// Log in {logIndex}/{logType} the synchronization results and metrics
        /// </summary>
        private SyncResponse Log(SyncResponse syncResponse)
        {
            stopwatch.Start();
            syncResponse.EndedOn = DateTime.UtcNow;
            var logBulk = String.Format("{0}{1}", ElasticsearchHelpers.GetPartialIndexBulk(LogIndex, LogType, new
            {
                startedOn = syncResponse.StartedOn,
                endedOn = syncResponse.EndedOn,
                success = syncResponse.Success,
                indexedDocuments = syncResponse.IndexedDocuments,
                deletedDocuments = syncResponse.DeletedDocuments,
                bulks = syncResponse.BulkResponses.Select(x => new
                {
                    success = x.Success,
                    httpStatusCode = x.HttpStatusCode,
                    affectedDocuments = x.AffectedDocuments,
                    duration = x.Duration + "ms",
                    exception = x.ESexception != null ? ((Exception)x.ESexception).Message : null
                })
            }),
            ElasticsearchHelpers.GetPartialIndexBulk(LogIndex, LastLogType, LastLogID, new
            {
                date = syncResponse.StartedOn
            }));
            client.Bulk(logBulk);

            stopwatch.Stop();
            log.Debug(String.Format("log index duration: {0}ms", stopwatch.ElapsedMilliseconds));
            stopwatch.Reset();

            return syncResponse;
        }

        private Dictionary<object, Dictionary<string, object>> GetSerializedObject()
        {
            try
            {
                _config.SqlConnection.Open();
                Dictionary<object, Dictionary<string, object>> data = null;
                _config.SqlCommand.CommandTimeout = 0;

                stopwatch.Start();
                using (SqlDataReader rdr = _config.SqlCommand.ExecuteReader())
                {
                    stopwatch.Stop();
                    log.Debug(String.Format("sql execute reader duration: {0}ms", stopwatch.ElapsedMilliseconds));
                    stopwatch.Reset();

                    data = rdr.Serialize(_config.XmlFields);
                }

                if (!data.Any())
                    return data;

                var dataIds = data.Select(x => "'" + x.Key + "'").ToArray();

                foreach (var arrayConfig in _config.ArraysConfiguration)
                {
                    arrayConfig.SqlCommand.CommandTimeout = 0;
                    stopwatch.Start();
                    using (SqlDataReader rdr = arrayConfig.SqlCommand.ExecuteReader())
                    {
                        stopwatch.Stop();
                        log.Debug(String.Format("array sql execute reader duration: {0}ms", stopwatch.ElapsedMilliseconds));
                        stopwatch.Reset();

                        data = rdr.SerializeArray(data, arrayConfig.AttributeName, arrayConfig.XmlFields, arrayConfig.InsertIntoArrayComparerKey);
                    }
                }

                foreach (var objectConfig in _config.ObjectsConfiguration)
                {
                    objectConfig.SqlCommand.CommandTimeout = 0;
                    stopwatch.Start();
                    using (SqlDataReader rdr = objectConfig.SqlCommand.ExecuteReader())
                    {
                        stopwatch.Stop();
                        log.Debug(String.Format("object sql execute reader duration: {0}ms", stopwatch.ElapsedMilliseconds));
                        stopwatch.Reset();

                        data = rdr.SerializeObject(data, objectConfig.AttributeName, objectConfig.InsertIntoArrayComparerKey);
                    }
                }

                return data;
            }
            finally
            {
                _config.SqlConnection.Close();
            }
        }

        private string AddSqlCondition(string sql, string condition)
        {
            return new StringBuilder(sql).Insert(
                sql.IndexOf("where", StringComparison.InvariantCultureIgnoreCase) + "where ".Length,
                new StringBuilder(condition).Append(" AND ").ToString()).ToString();
        }
    }
}