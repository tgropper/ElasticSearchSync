using System;
using System.Collections.Generic;

namespace ElasticSearchSync
{
    public class SyncResponse
    {
        public bool Success { get; set; }

        public int IndexedDocuments { get; set; }

        public int DeletedDocuments { get; set; }

        public DateTime StartedOn { get; set; }

        public DateTime EndedOn { get; set; }

        public List<BulkResponse> BulkResponses { get; set; }

        public SyncResponse(DateTime startedOn)
        {
            StartedOn = startedOn;
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

        public int AffectedDocuments { get; set; }

        public DateTime StartedOn { get; set; }

        /// <summary>
        /// Bulk duration in miliseconds
        /// </summary>
        public double Duration { get; set; }
    }
}