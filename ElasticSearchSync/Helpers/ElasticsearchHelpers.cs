using Newtonsoft.Json;
using System;

namespace ElasticSearchSync.Helpers
{
    public static class ElasticsearchHelpers
    {
        public static string GetPartialDeleteBulk(string index, string type, object id, object value = null)
        {
            return String.Format("{0}\n",
                JsonConvert.SerializeObject(new { delete = new { _index = index, _type = type, _id = id } }, Formatting.None));
        }

        public static string GetPartialIndexBulk(string index, string type, object value)
        {
            return String.Format("{0}\n{1}\n",
                JsonConvert.SerializeObject(new { index = new { _index = index, _type = type } }, Formatting.None),
                JsonConvert.SerializeObject(value, Formatting.None));
        }

        public static string GetPartialIndexBulk(string index, string type, object id, object value)
        {
            return String.Format("{0}\n{1}\n",
                JsonConvert.SerializeObject(new { index = new { _index = index, _type = type, _id = id } }, Formatting.None),
                JsonConvert.SerializeObject(value, Formatting.None));
        }
    }
}