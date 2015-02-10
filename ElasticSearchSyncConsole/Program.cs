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
            using (SqlConnection conn = new SqlConnection("Data Source=(local);Initial Catalog=sarasa;User Id=sa;Password=1234;Connect Timeout=120"))
            {
                SqlCommand cmd = new SqlCommand(@"
                    SELECT * FROM sarasa WHERE sarasa.desc LIKE '%asd%'"
                    , conn);

                List<SqlCommand> arrayCmd = new List<SqlCommand>()
                {
                    new SqlCommand(@"
                        SELECT * FROM sarasaArray WHERE sarasaArray.language = 'es'"
                    , conn)
                };
                var node = new Uri("http://localhost:9200");
                var esConfig = new ConnectionConfiguration(node).UsePrettyResponses(); //can configure exception handlers (by httpStatusCode)

                var syncConfig = new SyncConfiguration()
                {
                    SqlConnection = conn,
                    SqlCommand = cmd,
                    ArraySqlCommands = arrayCmd,
                    FilterArrayByParentsIds = true,
                    ParentIdColumn = "_id",
                    ColumnsToCompareWithLastSyncDate = new string[] { "[lastupdate]" },
                    DeleteSqlCommand = null, //deleteCmd,
                    ElasticSearchConfiguration = esConfig,
                    BulkSize = 500,
                    _Index = "sarasa",
                    _Type = "notes"
                };

                var sync = new Sync(syncConfig);

                try
                {
                    var response = sync.Exec();

                    foreach (var bulkResponse in response.BulkResponses)
                    {
                        Console.WriteLine("success: " + bulkResponse.Success);
                        Console.WriteLine("http status code: " + bulkResponse.HttpStatusCode);
                        Console.WriteLine("indexed documents: " + bulkResponse.DocumentsIndexed);
                        Console.WriteLine("deleted documents: " + bulkResponse.DocumentsDeleted);
                        Console.WriteLine("started on: " + bulkResponse.StartedOn);
                        Console.WriteLine("bulk duration: " + bulkResponse.Duration + "ms");
                        if (!response.Success)
                            Console.WriteLine("es original exception: " + bulkResponse.ESexception);
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
