using Elasticsearch.Net;
using Elasticsearch.Net.Connection;
using ElasticSearchSync.Helpers;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace ElasticSearchSync
{
    public class Program
    {
        static void Main(string[] args)
        {

            SqlConnection conn = new SqlConnection("Data Source=(local);Initial Catalog=FOXratingsPOCOSDATOS;User Id=sa;Password=1234;");
            SqlDataReader rdr = null;

            try
            {
                conn.Open();

                SqlCommand cmd = new SqlCommand(@"
                    SELECT TOP 3 mg.ID AS '_id',
              mg.ID AS 'MediaGroup._id', 
			  mg.Description AS 'MediaGroup.Description'
FROM dbo.ChannelMediaGroup mg
WHERE mg.CountryID IS NULL"
                    , conn);

                rdr = cmd.ExecuteReader();
                var data = rdr.Serialize();
                rdr.Close();
                
                cmd = new SqlCommand(String.Format(@"
                    SELECT TOP 6 mg.ID AS '_id',
			  nw.Description AS 'MediaGroup.Network.Description',
			  ct.IsoCode AS 'MediaGroup.Network.Country.IsoCode'
            FROM dbo.ChannelMediaGroup mg
            JOIN dbo.ChannelNetwork nw ON mg.ID = nw.MediaGroupID
            JOIN dbo.Country ct ON ct.ID = nw.CountryID
            WHERE mg.ID IN ({0}) AND mg.CountryID IS NULL", String.Join(",", data.Keys))
                     , conn);
                var arrayrdr = cmd.ExecuteReader();

                data = arrayrdr.SerializeArray(data);
                arrayrdr.Close();

                string bulk = "";
                foreach (var bulkData in data)
                {
                    bulk = bulk + String.Format("{0}\n{1}\n",
                        JsonConvert.SerializeObject(new { index = new { _index = "ratings", _type = "mediaGroup", _id = bulkData.Key } }, Formatting.None),
                        JsonConvert.SerializeObject(bulkData.Value, Formatting.None));
                }

                Console.WriteLine(bulk);

                var node = new Uri("http://localhost:9200"); //config
                var config = new ConnectionConfiguration(node).UsePrettyResponses();
                var client = new ElasticsearchClient(config);

                var response = client.Bulk(bulk);

                Console.WriteLine(response.Success);
                Console.WriteLine(response.HttpStatusCode);

                Console.ReadLine();
            }
            finally
            {
                if (conn != null)
                    conn.Close();
            }
        }
    }
}
