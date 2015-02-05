using System.Collections.Generic;

namespace ElasticSearchSync
{
    public class SyncResponse
    {
        public string Bulk { get; set; }

        public bool Success { get; set; }

        public int DocumentsIndexed { get; set; }

        public int DocumentsDeleted { get; set; }

        public List<BulkResponse> BulkResponses { get; set; }

        public SyncResponse()
        {
            Success = true;
            BulkResponses = new List<BulkResponse>();
        }
    }

    public class BulkResponse
    {
        public bool Success { get; set; }

        public int? HttpStatusCode { get; set; }

        /// <summary>
        /// Original ES exception
        /// </summary>
        public object ESexception { get; set; }

        public int DocumentsIndexed { get; set; }

        public int DocumentsDeleted { get; set; }

    }
}
