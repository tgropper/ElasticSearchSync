using Elasticsearch.Net;
using System;
using System.Globalization;

namespace ElasticSearchSync
{
    public class SyncLock : IDisposable
    {
        private const string _id = "1";

        public ElasticsearchClient Client { get; set; }

        public string LockIndex { get; set; }

        public string LockType { get; set; }

        public bool Force { get; set; }

        public SyncLock(ElasticsearchClient client, string index, string type, bool force = false)
        {
            Client = client;
            LockIndex = index;
            LockType = type;
            Force = force;

            Open();
        }

        private void Open()
        {
            if (Force)
                return;

            var body = new 
            { 
                date = DateTime.UtcNow
            };

            var _lock = Client.Get(LockIndex, LockType, _id);
            if (!bool.Parse(_lock.Response["found"]))
            {
                _lock = Client.Index(LockIndex, LockType, _id, body, q => q.OpType(OpType.Create));
                if (!_lock.Success)
                    throw new SyncConcurrencyException(_lock.OriginalException.Message);
            }
            else
            {
                DateTime lockDate = DateTime.ParseExact(
                    _lock.Response["_source"].date,
                    "yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal |
                    DateTimeStyles.AdjustToUniversal);
                var duration = Helpers.ConfigSection.Default.Concurrency.Duration;

                if (duration == null || lockDate + duration >= body.date)
                    throw new SyncConcurrencyException();

                Client.Delete(LockIndex, LockType, _id);
                _lock = Client.Index(LockIndex, LockType, _id, body, q => q.OpType(OpType.Create));
                if (!_lock.Success)
                    throw new SyncConcurrencyException(_lock.OriginalException.Message);
            }
        }

        public void Dispose()
        {
            if (Force)
                return;

            var d = Client.Delete(LockIndex, LockType, _id);
            if (!d.Success)
                throw new Exception(d.OriginalException.Message);
        }

        public class SyncConcurrencyException : Exception
        {
            public SyncConcurrencyException()
                : base() { }

            public SyncConcurrencyException(string message)
                : base(message) { }
        }
    }
}