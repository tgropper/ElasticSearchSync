using Elasticsearch.Net.Connection;
using Elasticsearch.Net.ConnectionPool;
using ElasticSearchSync;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace ElasticSearchSyncConsole
{
    public class Program
    {
        private static void Main(string[] args)
        {
            using (SqlConnection conn = new SqlConnection("Data Source=(local);Initial Catalog=sarasa;User Id=sa;Password=1234;Connect Timeout=120"))
            {
                SqlCommand cmd = new SqlCommand(@"
                    SELECT * FROM notes WHERE note.language = 'es'"
                    , conn);

                List<SyncArrayConfiguration> arrayConfig = new List<SyncArrayConfiguration>()
                {
                    new SyncArrayConfiguration
                    {
                        SqlCommand = new SqlCommand(@"
                            SELECT id_object AS '_id', id, description, languageId
                            FROM dbo.Tags
                            WHERE languageId = 'es'"
                            , conn),
                        AttributeName = "tags",
                        ParentIdColumn = "_id"
                    },
                    new SyncArrayConfiguration
                    {
                        SqlCommand = new SqlCommand(@"
                            SELECT id_object AS '_id', id, description, xmlData, languageId
                            FROM dbo.Categories
                            WHERE languageId = 'es'"
                            , conn),
                        AttributeName = "categories",
                        ParentIdColumn = "_id",
                        XmlFields = new string[] { "xmlData" }
                    }
                };

                SyncDeleteConfiguration deleteCmd = new SyncDeleteConfiguration
                {
                    SqlCommand = new SqlCommand(@"
                        SELECT id_object AS '_id', createdOn
                        FROM dbo.DeleteLog
                        WHERE id_language = 'es'"
                        , conn),
                    ColumnsToCompareWithLastSyncDate = new string[] { "[createdOn]" },
                };

                var nodes =
                    new Uri[] {
                        new Uri("http://localhost:9200"),
                        new Uri("http://localhost:9201")
                    };
                var connectionPool = new SniffingConnectionPool(nodes);
                var esConfig = new ConnectionConfiguration(connectionPool).UsePrettyResponses();

                var syncConfig = new SyncConfiguration()
                {
                    SqlConnection = conn,
                    SqlCommand = cmd,
                    ArraysConfiguration = arrayConfig,
                    FilterArrayByParentsIds = true,
                    ColumnsToCompareWithLastSyncDate = new string[] { "[lastupdate]" },
                    DeleteConfiguration = deleteCmd,
                    ElasticSearchConfiguration = esConfig,
                    BulkSize = 500,
                    _Index = "sarasa",
                    _Type = "notes"
                };

                var sync = new Sync(syncConfig);

                try
                {
                    var response = sync.Exec();
                    if (response.BulkResponses.Any())
                    {
                        foreach (var bulkResponse in response.BulkResponses)
                        {
                            Console.WriteLine("success: " + bulkResponse.Success);
                            Console.WriteLine("http status code: " + bulkResponse.HttpStatusCode);
                            Console.WriteLine("affected documents: " + bulkResponse.AffectedDocuments);
                            Console.WriteLine("started on: " + bulkResponse.StartedOn);
                            Console.WriteLine("bulk duration: " + bulkResponse.Duration + "ms");
                            if (!response.Success)
                                Console.WriteLine("es original exception: " + bulkResponse.ESexception);
                            Console.WriteLine("\n");
                        }
                        Console.WriteLine("\nbulk avg duration: " + response.BulkResponses.Average(x => x.Duration) + "ms");
                        Console.WriteLine("\n");
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