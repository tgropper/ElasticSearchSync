using Elasticsearch.Net;
using System;

namespace ElasticSearchSync
{
    public class SyncLock : IDisposable
    {
        private const string _id = "1";

        public ElasticsearchClient Client { get; set; }

        public string LockIndex { get; set; }

        public string LockType { get; set; }

        public SyncLock(ElasticsearchClient client, string index, string type)
        {
            Client = client;
            LockIndex = index;
            LockType = type;

            Open();
        }

        private void Open()
        {
            var r = Client.Index(LockIndex, LockType, _id, new object(), q => q.OpType(OpType.Create));
            if (r.HttpStatusCode == 409)
                throw new SyncConcurrencyException();
        }

        public void Dispose()
        {
            Client.Delete(LockIndex, LockType, _id);
        }

        public class SyncConcurrencyException : Exception
        {
            public SyncConcurrencyException()
                : base() { }
        }
    }
}