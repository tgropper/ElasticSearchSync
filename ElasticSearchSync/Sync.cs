using Elasticsearch.Net;
using ElasticSearchSync.Helpers;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace ElasticSearchSync
{
    public class Sync
    {
        private SyncConfiguration Config;
        public Sync(SyncConfiguration config)
        {
            Config = config;
        }

        private Dictionary<object, Dictionary<string, object>> GetSerializedObject()
        { 
            try
            {
                this.Config.SqlConnection.Open();
                Dictionary<object, Dictionary<string, object>> data = null;
                using (SqlDataReader rdr = this.Config.SqlCommand.ExecuteReader())
                {
                    data = rdr.Serialize();
                }

                foreach (var cmd in this.Config.ArraySqlCommands)
                    using (SqlDataReader rdr = cmd.ExecuteReader())
                    {
                        data = rdr.SerializeArray(data);
                    }

                return data;
            }
            finally
            {
                this.Config.SqlConnection.Close();
            }
        }

        public string GetBulk(Dictionary<object, Dictionary<string, object>> data)
        {
            string bulk = "";
            foreach (var bulkData in data)
                bulk = bulk + GetPartialBulk(bulkData.Key, bulkData.Value);
            return bulk;
        }

        private string GetPartialBulk(object key, Dictionary<string, object> value)
        {
            return String.Format("{0}\n{1}\n",
                JsonConvert.SerializeObject(new { index = new { _index = this.Config._Index, _type = this.Config._Type, _id = key } }, Formatting.None),
                JsonConvert.SerializeObject(value, Formatting.None));
        }

        public SyncResponse Exec()
        {
            var started = DateTime.UtcNow;
            var data = GetSerializedObject();
            var client = new ElasticsearchClient(this.Config.ElasticSearchConfiguration);

            var syncResponse = new SyncResponse();
            string partialbulk = string.Empty;
            var c = 0;
            while (c < data.Count())
            {
                var partialData = data.Skip(c).Take(this.Config.BulkSize).ToList();
                foreach (var bulkData in partialData)
                    partialbulk = partialbulk + GetPartialBulk(bulkData.Key, bulkData.Value);

                //bulk request
                var response = client.Bulk(partialbulk);

                //log
                syncResponse.Bulk = syncResponse.Bulk + partialbulk;
                var indexedDocuments = response.Response["items"].HasValue ? ((object[])response.Response["items"].Value).Length : 0;
                syncResponse.BulkResponses.Add(new BulkResponse
                {
                    Success = response.Success,
                    HttpStatusCode = response.HttpStatusCode,
                    DocumentsIndexed = indexedDocuments,
                    ESexception = response.OriginalException
                });

                syncResponse.DocumentsIndexed += indexedDocuments;
                syncResponse.Success = syncResponse.Success && response.Success;

                partialbulk = string.Empty;
                c += this.Config.BulkSize;
            }

            client.Bulk("sqlserver_es_sync", new object[]
            { 
                new { create = new { _type = "log"  } },
                new
                { 
                    started = started,
                    ended = DateTime.UtcNow,
                    success = syncResponse.Success,
                    indexedDocuments = syncResponse.DocumentsIndexed,
                    bulks = syncResponse.BulkResponses.Select(x => new {
                        success = x.Success,
                        httpStatusCode = x.HttpStatusCode,
                        indexedDocuments = x.DocumentsIndexed,
                        exception = x.ESexception != null ? ((Exception)x.ESexception).Message : null
                    })
                }
            });

            return syncResponse;
        }
    }
}