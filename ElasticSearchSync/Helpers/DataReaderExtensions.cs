using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace ElasticSearchSync.Helpers
{
    public static class DataReaderExtensions
    {
        /// <summary>
        /// Serialize SqlDataReader into a json serializable dictionary, with document _id as key.
        /// </summary>
        public static Dictionary<object, Dictionary<string, object>> Serialize(this SqlDataReader reader)
        {
            var results = new Dictionary<object, Dictionary<string, object>>();
            var cols = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
                cols.Add(reader.GetName(i));

            while (reader.Read())
            {
                var r = SerializeRow(cols, reader);
                results.Add(r.Values.First(), r);
            }

            return results;
        }

        public static Dictionary<object, Dictionary<string, object>> SerializeArray(
            this SqlDataReader reader,
            Dictionary<object, Dictionary<string, object>> results,
            string attributeName)
        {
            var cols = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
                cols.Add(reader.GetName(i));

            while (reader.Read())
            {
                var r = SerializeRow(cols, reader);
                if (!results.ContainsKey(r.Values.First()))
                    throw new Exception(String.Format("Array element is related with an object with _id {0}, and it doesn't belong to serialized objects list", r.Values.First()));

                var _object = results[r.Values.First()];
                r = r.Skip(1).ToDictionary(x => x.Key, x => x.Value);
                var elem = GetElementPosition(_object, attributeName);
                //var arrayElemKey = ((Dictionary<string, object>)r.Values.First()).Keys.First();
                var arrayElemKey = GetElementName(attributeName);
                if (!elem.ContainsKey(arrayElemKey))
                    elem.Add(arrayElemKey, new List<object>());

                ((List<object>)elem[arrayElemKey]).Add(r);
            }

            return results;
        }

        private static Dictionary<string, object> SerializeRow(IEnumerable<string> cols, SqlDataReader reader)
        {
            var result = new Dictionary<string, object>();
            foreach (var col in cols)
            {
                SerializeObject(col, col, reader, result);
            }
            return result;
        }

        private static Dictionary<string, object> SerializeObject(
            string col,
            string fullColName,
            SqlDataReader reader,
            Dictionary<string, object> result)
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
                    (Dictionary<string, object>)result[objName]);
                return result;
            }
            else
            {
                var val = reader[fullColName];
                result[col] = val;
                return result;
            }
        }

        private static Dictionary<string, object> GetElementPosition(Dictionary<string, object> _object, string property)
        {
            var index = property.IndexOf('.');
            if (index != -1)
                return GetElementPosition((Dictionary<string, object>)_object[property.Substring(0, index)], property.Substring(index + 1));
            else
                return _object;
        }

        private static string GetElementName(string property)
        {
            var index = property.LastIndexOf('.');
            if (index != -1)
                return property.Substring(index + 1);
            else
                return property;
        }
    }
}