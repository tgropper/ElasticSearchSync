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
                SqlCommand cmd = new SqlCommand(@"
                SELECT TOP 3 mg.ID AS '_id',
                            mg.ID AS 'MediaGroup._id', 
			                mg.Description AS 'MediaGroup.Description'
                FROM dbo.ChannelMediaGroup mg
                WHERE mg.CountryID IS NULL
                ORDER BY mg.ID"
                    , conn);

                List<SqlCommand> arrayCmd = new List<SqlCommand>()
                {
                    new SqlCommand(String.Format(@"
                        SELECT TOP 4 mg.ID AS '_id',
			                    nw.Description AS 'MediaGroup.Network.Description',
			                    ct.IsoCode AS 'MediaGroup.Network.Country.IsoCode'
                            FROM dbo.ChannelMediaGroup mg
                            JOIN dbo.ChannelNetwork nw ON mg.ID = nw.MediaGroupID
                            JOIN dbo.Country ct ON ct.ID = nw.CountryID
                            WHERE mg.CountryID IS NULL AND mg.ID IN (SELECT TOP 3 ID
                                FROM dbo.ChannelMediaGroup
                                WHERE CountryID IS NULL
                                ORDER BY ID)")
                                            , conn),
                    new SqlCommand(String.Format(@"
                        SELECT TOP 4 mg.ID AS '_id',
	                        rat.ID AS 'MediaGroup.Rating.ID',
	                        rat.RatingPerc AS 'MediaGroup.Rating.Perc',
	                        tg.Description AS 'MediaGroup.Rating.Target.Description'
                        FROM dbo.ChannelMediaGroup mg
                        JOIN dbo.ChannelRating rat ON mg.ID = rat.MediaGroupID
                        JOIN dbo.ChannelTarget tg ON rat.TargetID = tg.ID
                        WHERE mg.CountryID IS NULL AND rat.RatingPerc <> 0 AND mg.ID IN (SELECT TOP 3 ID
                            FROM dbo.ChannelMediaGroup
                            WHERE CountryID IS NULL
                            ORDER BY ID)")
                                            , conn)
                };
                var node = new Uri("http://localhost:9200");
                var esConfig = new ConnectionConfiguration(node).UsePrettyResponses(); //can configure exception handlers (by httpStatusCode)

                var syncConfig = new SyncConfiguration()
                {
                    SqlConnection = conn,
                    SqlCommand = cmd,
                    ArraySqlCommands = arrayCmd,
                    ElasticSearchConfiguration = esConfig,
                    _Index = "ratings",
                    _Type = "mediagroups"
                };

                var sync = new Sync(syncConfig);

                try
                {
                    var response = sync.Exec();

                    Console.WriteLine(response.Bulk);
                    Console.WriteLine("success: " + response.Success);
                    Console.WriteLine("http status code: " + response.HttpStatusCode);
                    Console.WriteLine("indexed documents: " + response.DocumentsIndexed);
                    if (!response.Success)
                        Console.WriteLine("es original exception: " + response.ESexception);
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
