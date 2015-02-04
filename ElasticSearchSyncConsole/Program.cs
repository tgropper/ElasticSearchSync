using Elasticsearch.Net.Connection;
using ElasticSearchSync;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace ElasticSearchSyncConsole
{
    public class Program
    {
        static void Main(string[] args)
        {
            using (SqlConnection conn = new SqlConnection("Data Source=(local);Initial Catalog=FOXratings;User Id=sa;Password=1234;"))
            {
//                SqlCommand cmd = new SqlCommand(@"
//                SELECT mg.ID AS '_id',
//                            mg.ID AS 'MediaGroup._id', 
//			                mg.Description AS 'MediaGroup.Description'
//                FROM dbo.ChannelMediaGroup mg
//                ORDER BY mg.ID"
//                    , conn);
                SqlCommand cmd = new SqlCommand(@"
                SELECT TOP(1500) rat.ID AS '_id', 
	                rat.RatingPerc AS 'Rating.Perc',
	                CONVERT(DATE, up.Month) AS 'Rating.Month',
	                tg.Description AS 'Rating.Target.Description',
	                dp.Description AS 'Rating.DayPart.Description',
	                nw.ID AS 'Rating.Network.ID',
	                nw.Description AS 'Rating.Network.Description'
                FROM dbo.ChannelRating rat
                JOIN dbo.ChannelTarget tg ON tg.ID = rat.TargetID
                JOIN dbo.ChannelDayPart dp ON dp.ID = rat.DayPartID
                JOIN dbo.ChannelNetwork nw ON nw.ID = rat.NetworkID
                JOIN dbo.ChannelUpload up ON up.ID = rat.UploadID"
                    , conn);

                List<SqlCommand> arrayCmd = new List<SqlCommand>()
                {
//                    new SqlCommand(String.Format(@"
//                        SELECT mg.ID AS '_id',
//			                    nw.Description AS 'MediaGroup.Network.Description',
//			                    ct.IsoCode AS 'MediaGroup.Network.Country.IsoCode'
//                            FROM dbo.ChannelMediaGroup mg
//                            JOIN dbo.ChannelNetwork nw ON mg.ID = nw.MediaGroupID
//                            JOIN dbo.Country ct ON ct.ID = nw.CountryID
//                            WHERE mg.CountryID IS NULL AND mg.ID IN (SELECT TOP 3 ID
//                                FROM dbo.ChannelMediaGroup
//                                WHERE CountryID IS NULL)")
//                                            , conn),
//                    new SqlCommand(String.Format(@"
//                        SELECT mg.ID AS '_id',
//	                        rat.ID AS 'MediaGroup.Rating.ID',
//	                        rat.RatingPerc AS 'MediaGroup.Rating.Perc',
//	                        tg.Description AS 'MediaGroup.Rating.Target.Description'
//                        FROM dbo.ChannelMediaGroup mg
//                        JOIN dbo.ChannelRating rat ON mg.ID = rat.MediaGroupID
//                        JOIN dbo.ChannelTarget tg ON rat.TargetID = tg.ID
//                        WHERE mg.CountryID IS NULL AND rat.RatingPerc <> 0 AND mg.ID IN (SELECT ID
//                            FROM dbo.ChannelMediaGroup
//                            WHERE CountryID IS NULL)")
//                                            , conn)
                };
                var node = new Uri("http://localhost:9200");
                var esConfig = new ConnectionConfiguration(node).UsePrettyResponses(); //can configure exception handlers (by httpStatusCode)

                var syncConfig = new SyncConfiguration()
                {
                    SqlConnection = conn,
                    SqlCommand = cmd,
                    ArraySqlCommands = arrayCmd,
                    ElasticSearchConfiguration = esConfig,
                    BulkSize = 500,
                    _Index = "ratings",
                    _Type = "ratings"
                };

                var sync = new Sync(syncConfig);

                try
                {
                    var response = sync.Exec();

                    Console.WriteLine(response.Bulk);
                    foreach (var bulkResponse in response.BulkResponses)
                    {
                        Console.WriteLine("success: " + bulkResponse.Success);
                        Console.WriteLine("http status code: " + bulkResponse.HttpStatusCode);
                        Console.WriteLine("indexed documents: " + bulkResponse.DocumentsIndexed);
                        if (!response.Success)
                            Console.WriteLine("es original exception: " + bulkResponse.ESexception);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("an error has occurred: " + ex.Message);
                }
                finally
                { 
                    Console.WriteLine("Execution has completed. Press any key to continue...");
                    Console.ReadKey();
                }

            }
        }
    }
}
