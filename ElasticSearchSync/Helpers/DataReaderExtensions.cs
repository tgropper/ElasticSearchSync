using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace ElasticSearchSync.Helpers
{
    public static class DataReaderExtensions
    {
        public static IEnumerable<Dictionary<string, object>> Serialize(this SqlDataReader reader)
        {
            var results = new List<Dictionary<string, object>>();
            var cols = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
                cols.Add(reader.GetName(i));

            while (reader.Read())
            {
                var r = SerializeRow(cols, reader, results);
                results.Add(r);
            }

            return results;
        }

        private static Dictionary<string, object> SerializeRow(IEnumerable<string> cols, SqlDataReader reader, IEnumerable<Dictionary<string, object>> results)
        {
            var result = new Dictionary<string, object>();
            foreach (var col in cols)
            {
                SerializeObject(col, col, reader, result, results);
            }
            return result;
        }

        private static Dictionary<string, object> SerializeObject(
            string col,
            string fullColName,
            SqlDataReader reader,
            Dictionary<string, object> result,
            IEnumerable<Dictionary<string, object>> results)
        {
            var objIndex = col.IndexOf('.');
            if (objIndex != -1)
            {
                var objName = col.Substring(0, objIndex);
                if (!result.ContainsKey(objName))
                    result.Add(objName, new Dictionary<string, object>());

                SerializeObject(
                    col.Substring(objIndex + 1),
                    fullColName,
                    reader,
                    (Dictionary<string, object>)result[objName],
                    results.Where(x => x.ContainsKey(objName)));
                return result;
            }
            else
            {
                var val = reader[fullColName];

                result[col] = val;
                return result;
            }
        }
    }
}
