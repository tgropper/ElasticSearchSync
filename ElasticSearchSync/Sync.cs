using Bardock.Utils.Extensions;
using Elasticsearch.Net;
using ElasticSearchSync.Helpers;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace ElasticSearchSync
{
    public class Sync
    {
        public log4net.ILog log { get; set; }

        private Stopwatch stopwatch { get; set; }

        private SyncConfiguration Config;
        private const string LogIndex = "sqlserver_es_sync";
        private const string LogType = "log";
        private const string BulkLogType = "bulk_log";

        public Sync(SyncConfiguration config)
        {
            Config = config;
            log4net.Config.BasicConfigurator.Configure();
            log = log4net.LogManager.GetLogger("SQLSERVER-ES Sync");
            stopwatch = new Stopwatch();
        }

        public SyncResponse Exec()
        {
            var startedOn = DateTime.UtcNow;
            log.Info("process started at " + startedOn.NormalizedFormat());
            var client = new ElasticsearchClient(this.Config.ElasticSearchConfiguration);

            DateTime? lastSyncDate = null;
            if (this.Config.ColumnsToCompareWithLastSyncDate != null)
            {
                client.ClusterHealth();
                stopwatch.Start();
                var lastSyncResponse = client.Search(LogIndex, LogType, @"{
                  ""filter"" : {
                    ""match_all"" : { }
                  },
                  ""sort"": [{
                      ""startedOn"": {
                        ""order"": ""desc""
                      }
                    }
                  ],
                  ""size"": 1
                }");
                stopwatch.Stop();
                log.Debug(String.Format("last sync search duration: {0}ms", stopwatch.ElapsedMilliseconds));
                stopwatch.Reset();

                lastSyncDate = lastSyncResponse.Response != null
                    ? DateTime.Parse(lastSyncResponse.Response["hits"]["hits"]["_index"][0]["_source"]["startedOn"]).ToUniversalTime()
                    : null;

                if (lastSyncDate != null)
                {
                    var conditionBuilder = new StringBuilder("(");
                    foreach (var col in this.Config.ColumnsToCompareWithLastSyncDate)
                        conditionBuilder
                            .Append(col)
                            .Append(" >= '")
                            .Append(lastSyncDate.Value.NormalizedFormat())
                            .Append("' OR ");
                    conditionBuilder.RemoveLastCharacters(4).Append(")");

                    this.Config.SqlCommand.CommandText = AddSqlCondition(this.Config.SqlCommand.CommandText, conditionBuilder.ToString());
                }
                else
                    this.Config.FilterArrayByParentsIds = false;
            }

            var data = GetSerializedObject();
            log.Info(String.Format("{0} objects have been serialized.", data.Count()));

            var syncResponse = new SyncResponse(startedOn);

            string partialbulk = string.Empty;
            var c = 0;
            while (c < data.Count())
            {
                var bulkStartedOn = DateTime.UtcNow;
                stopwatch.Start();
                var partialData = data.Skip(c).Take(this.Config.BulkSize).ToList();
                foreach (var bulkData in partialData)
                    partialbulk = partialbulk + GetPartialIndexBulk(bulkData.Key, bulkData.Value);

                //bulk request
                var response = client.Bulk(partialbulk);
                stopwatch.Stop();

                //log
                var indexedDocuments = response.Response["items"].HasValue ? ((object[])response.Response["items"].Value).Length : 0;
                var bulkResponse = new BulkResponse
                {
                    Success = response.Success,
                    HttpStatusCode = response.HttpStatusCode,
                    DocumentsIndexed = indexedDocuments,
                    ESexception = response.OriginalException,
                    StartedOn = bulkStartedOn,
                    Duration = stopwatch.ElapsedMilliseconds
                };
                syncResponse.BulkResponses.Add(bulkResponse);
                syncResponse.DocumentsIndexed += indexedDocuments;
                syncResponse.Success = syncResponse.Success && response.Success;

                client.IndexAsync(LogIndex, BulkLogType, new
                {
                    success = bulkResponse.Success,
                    httpStatusCode = bulkResponse.HttpStatusCode,
                    documentsIndexed = bulkResponse.DocumentsIndexed,
                    startedOn = bulkResponse.StartedOn,
                    duration = bulkResponse.Duration + "ms",
                    exception = bulkResponse.ESexception != null ? ((Exception)bulkResponse.ESexception).Message : null
                });
                log.Info(String.Format("bulk duration: {0}ms. so far {1} documents have been indexed successfully.", bulkResponse.Duration, syncResponse.DocumentsIndexed));

                stopwatch.Reset();
                partialbulk = string.Empty;
                c += this.Config.BulkSize;
            }

            //EXTRACT METHOD
            if (this.Config.DeleteSqlCommand != null)
            {
                this.Config.SqlConnection.Open();
                Dictionary<object, Dictionary<string, object>> deleteData = null;

                using (SqlDataReader rdr = this.Config.DeleteSqlCommand.ExecuteReader())
                {
                    deleteData = rdr.Serialize();
                }
                this.Config.SqlConnection.Close();

                var d = 0;
                while (d < deleteData.Count())
                {
                    var bulkStartedOn = DateTime.UtcNow;
                    var partialData = deleteData.Skip(d).Take(this.Config.BulkSize).ToList();
                    foreach (var bulkData in partialData)
                        partialbulk = partialbulk + GetPartialDeleteBulk(bulkData.Key);

                    //bulk request
                    var response = client.Bulk(partialbulk);
                    stopwatch.Stop();

                    //log
                    var deletedDocuments = response.Response["items"].HasValue ? ((object[])response.Response["items"].Value).Length : 0;
                    var bulkResponse = new BulkResponse
                    {
                        Success = response.Success,
                        HttpStatusCode = response.HttpStatusCode,
                        DocumentsDeleted = deletedDocuments,
                        ESexception = response.OriginalException,
                        StartedOn = bulkStartedOn,
                        Duration = stopwatch.ElapsedMilliseconds
                    };
                    syncResponse.BulkResponses.Add(bulkResponse);
                    client.IndexAsync(LogIndex, BulkLogType, new
                    {
                        success = bulkResponse.Success,
                        httpStatusCode = bulkResponse.HttpStatusCode,
                        documentsDeleted = bulkResponse.DocumentsDeleted,
                        startedOn = bulkResponse.StartedOn,
                        duration = bulkResponse.Duration + "ms",
                        exception = bulkResponse.ESexception != null ? ((Exception)bulkResponse.ESexception).Message : null
                    });
                    log.Info(String.Format("bulk duration: {0}ms. so far {1} documents have been deleted successfully.", bulkResponse.Duration, syncResponse.DocumentsDeleted));

                    syncResponse.DocumentsDeleted += deletedDocuments;
                    syncResponse.Success = syncResponse.Success && response.Success;

                    stopwatch.Reset();
                    partialbulk = string.Empty;
                    d += this.Config.BulkSize;
                }
            }

            stopwatch.Start();
            client.Bulk(LogIndex, new object[]
            {
                new { create = new { _type = LogType  } },
                new
                {
                    startedOn = startedOn,
                    endedOn = DateTime.UtcNow,
                    success = syncResponse.Success,
                    indexedDocuments = syncResponse.DocumentsIndexed,
                    deletedDocuments = syncResponse.DocumentsDeleted,
                    bulks = syncResponse.BulkResponses.Select(x => new {
                        success = x.Success,
                        httpStatusCode = x.HttpStatusCode,
                        indexedDocuments = x.DocumentsIndexed,
                        deletedDocuments = x.DocumentsDeleted,
                        duration = x.Duration + "ms",
                        exception = x.ESexception != null ? ((Exception)x.ESexception).Message : null
                    })
                }
            });
            stopwatch.Stop();
            log.Debug(String.Format("log index duration: {0}ms", stopwatch.ElapsedMilliseconds));
            stopwatch.Reset();
            syncResponse.EndedOn = DateTime.UtcNow;

            log.Info(String.Format("process duration: {0}ms", Math.Truncate((syncResponse.EndedOn - syncResponse.StartedOn).TotalMilliseconds)));

            return syncResponse;
        }

        private Dictionary<object, Dictionary<string, object>> GetSerializedObject()
        {
            try
            {
                this.Config.SqlConnection.Open();
                Dictionary<object, Dictionary<string, object>> data = null;
                this.Config.SqlCommand.CommandTimeout = 0;

                stopwatch.Start();
                using (SqlDataReader rdr = this.Config.SqlCommand.ExecuteReader())
                {
                    stopwatch.Stop();
                    log.Debug(String.Format("sql execute reader duration: {0}ms", stopwatch.ElapsedMilliseconds));
                    stopwatch.Reset();

                    data = rdr.Serialize();
                }

                if (!data.Any())
                    return data;

                var dataIds = data.Select(x => "'" + x.Key + "'").ToArray();

                foreach (var arrayConfig in this.Config.ArraysConfiguration)
                {
                    arrayConfig.SqlCommand.CommandTimeout = 0;
                    if (this.Config.FilterArrayByParentsIds && arrayConfig.ParentIdColumn != null)
                    {
                        var conditionBuilder = new StringBuilder()
                            .Append(arrayConfig.ParentIdColumn)
                            .Append(" IN (")
                            .Append(String.Join(",", dataIds))
                            .Append(")");

                        arrayConfig.SqlCommand.CommandText = AddSqlCondition(arrayConfig.SqlCommand.CommandText, conditionBuilder.ToString());
                    }
                    stopwatch.Start();
                    using (SqlDataReader rdr = arrayConfig.SqlCommand.ExecuteReader())
                    {
                        stopwatch.Stop();
                        log.Debug(String.Format("array sql execute reader duration: {0}ms", stopwatch.ElapsedMilliseconds));
                        stopwatch.Reset();

                        data = rdr.SerializeArray(data, arrayConfig.AttributeName);
                    }
                }

                return data;
            }
            finally
            {
                this.Config.SqlConnection.Close();
            }
        }

        private string GetPartialDeleteBulk(object key)
        {
            return String.Format("{0}\n",
                JsonConvert.SerializeObject(new { delete = new { _index = this.Config._Index, _type = this.Config._Type, _id = key } }, Formatting.None));
        }

        private string GetPartialIndexBulk(object key, Dictionary<string, object> value)
        {
            return String.Format("{0}\n{1}\n",
                JsonConvert.SerializeObject(new { index = new { _index = this.Config._Index, _type = this.Config._Type, _id = key } }, Formatting.None),
                JsonConvert.SerializeObject(value, Formatting.None));
        }

        private string AddSqlCondition(string sql, string condition)
        {
            return new StringBuilder(sql).Insert(
                sql.IndexOf("where", StringComparison.InvariantCultureIgnoreCase) + "where ".Length,
                new StringBuilder(condition).Append(" AND ").ToString()).ToString();
        }
    }
}