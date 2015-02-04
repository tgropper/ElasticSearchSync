namespace ElasticSearchSync
{
    public class SyncResponse
    {
        public string Bulk { get; set; }

        public bool Success { get; set; }

        public int? HttpStatusCode { get; set; }
    }
}
