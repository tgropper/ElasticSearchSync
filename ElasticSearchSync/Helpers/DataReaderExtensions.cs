using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Xml;

namespace ElasticSearchSync.Helpers
{
    public static class DataReaderExtensions
    {
        /// <summary>
        /// Serialize SqlDataReader into a json serializable dictionary, with document _id as key.
        /// </summary>
        public static Dictionary<object, Dictionary<string, object>> Serialize(this SqlDataReader reader, string[] xmlFields = null)
        {
            var results = new Dictionary<object, Dictionary<string, object>>();
            var cols = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
                cols.Add(reader.GetName(i));

            while (reader.Read())
            {
                var r = SerializeRow(cols, reader, xmlFields);
                results.Add(r.Values.First(), r);
            }

            return results;
        }

        public static Dictionary<object, Dictionary<string, object>> SerializeArray(
            this SqlDataReader reader,
            Dictionary<object, Dictionary<string, object>> results,
            string attributeName,
            string[] xmlFields = null)
        {
            var cols = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
                cols.Add(reader.GetName(i));

            while (reader.Read())
            {
                var r = SerializeRow(cols, reader, xmlFields);
                if (!results.ContainsKey(r.Values.First()))
                    throw new Exception(String.Format("Array element is related with an object with _id {0}, and it doesn't belong to serialized objects list", r.Values.First()));

                var _object = results[r.Values.First()];
                r = r.Skip(1).ToDictionary(x => x.Key, x => x.Value);
                var elem = GetElementPosition(_object, attributeName);

                var arrayElemKey = GetElementName(attributeName);
                if (!elem.ContainsKey(arrayElemKey))
                    elem.Add(arrayElemKey, new List<object>());

                ((List<object>)elem[arrayElemKey]).Add(r);
            }

            return results;
        }

        private static Dictionary<string, object> SerializeRow(
            IEnumerable<string> cols, 
            SqlDataReader reader,
            string[] xmlFields = null)
        {
            var result = new Dictionary<string, object>();
            foreach (var col in cols)
            {
                SerializeObject(col, col, reader, result, xmlFields);
            }
            return result;
        }

        private static Dictionary<string, object> SerializeObject(
            string col,
            string fullColName,
            SqlDataReader reader,
            Dictionary<string, object> result,
            string[] xmlFields = null)
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
                if (xmlFields != null && xmlFields.Contains(fullColName))
                    val = SerializeXml(val.ToString());

                result[col] = val;
                return result;
            }
        }

        private static Dictionary<string, object> SerializeXml(
            string data)
        {
            XmlDocument xml = new XmlDocument();
            xml.LoadXml(data);

            RemoveCdata(xml);

            var json = Newtonsoft.Json.JsonConvert.SerializeXmlNode(xml, Newtonsoft.Json.Formatting.None, omitRootObject: true);

            return Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, object>>(json);
        }

        private static void RemoveCdata(XmlNode root)
        {
            foreach (XmlNode n in root.ChildNodes)
            {
                if (n.NodeType == XmlNodeType.CDATA)
                {
                    var data = n.Value;
                    root.RemoveChild(n); ;
                    root.InnerText = data;
                }
                else if (n.NodeType == XmlNodeType.Element)
                    RemoveCdata(n);
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