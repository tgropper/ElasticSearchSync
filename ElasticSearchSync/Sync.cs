using Elasticsearch.Net;
using ElasticSearchSync.Helpers;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace ElasticSearchSync
{
    public class Sync
    {
        private SyncConfiguration Config;
        public Sync(SyncConfiguration config)
        {
            Config = config;
        }

        public string GetBulk()
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

                string bulk = "";
                foreach (var bulkData in data)
                    bulk = bulk + String.Format("{0}\n{1}\n",
                        JsonConvert.SerializeObject(new { index = new { _index = this.Config._Index, _type = this.Config._Type, _id = bulkData.Key } }, Formatting.None),
                        JsonConvert.SerializeObject(bulkData.Value, Formatting.None));

                return bulk;
            }
            finally
            {
                this.Config.SqlConnection.Close();
            }
        }

        public SyncResponse Exec()
        {
            var bulk = GetBulk();
            var client = new ElasticsearchClient(this.Config.ElasticSearchConfiguration);

            var response = client.Bulk(bulk);
            client.Bulk("sql_es_sync", new object[]
            { 
                new { create = new { _type = "log"  } },
                new { date = DateTime.UtcNow, success = response.Success, statusCode = response.HttpStatusCode }
            });

            return new SyncResponse
            {
                Bulk = bulk,
                Success = response.Success,
                HttpStatusCode = response.HttpStatusCode
            };
        }
    }
}