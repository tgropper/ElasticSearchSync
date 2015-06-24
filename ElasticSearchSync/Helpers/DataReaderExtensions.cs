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

        /// <summary>
        /// Serialize an array and insert it into the specified primary object
        /// </summary>
        /// <param name="reader">Data Reader</param>
        /// <param name="results">List of primary objects</param>
        /// <param name="attributeName">Name of the object belonging to the primary object where the array will be inserted on</param>
        /// <param name="xmlFields">Array fields that are of type Xml</param>
        /// <param name="insertIntoArrayComparerKey">
        /// If it has value, it will be assumed that the attributeName parameter refers to an array existing within the primary object,
        /// and the new array will be inserted into it, using this parameter as key to access to each array object and comparing its value with
        /// the second property of each object in the new array
        /// </param>
        /// <returns>List of primary objects with arrays processed and inserted into it</returns>
        public static Dictionary<object, Dictionary<string, object>> SerializeArray(
            this SqlDataReader reader,
            Dictionary<object, Dictionary<string, object>> results,
            string attributeName,
            string[] xmlFields = null,
            string insertIntoArrayComparerKey = null)
        {
            var cols = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
                cols.Add(reader.GetName(i));

            while (reader.Read())
            {
                var serializedRow = SerializeRow(cols, reader, xmlFields);
                if (!results.ContainsKey(serializedRow.Values.First()))
                    throw new Exception(String.Format("Array element is related with an object with _id {0}, but it doesn't belong to serialized objects list", serializedRow.Values.First()));

                var _object = results[serializedRow.Values.First()];
                serializedRow = serializedRow.Skip(1).ToDictionary(x => x.Key, x => x.Value);

                var newArrayKey = GetLeafName(attributeName);
                Dictionary<string, object> newArrayContainerElement = null;
                if (insertIntoArrayComparerKey != null)
                {
                    var existingArrayAttributeName = attributeName.Substring(0, attributeName.Count() - newArrayKey.Count() - 1);
                    var existingArrayContainerElement = GetLeafContainerElement(_object, existingArrayAttributeName);
                    var existingArrayKey = GetLeafName(existingArrayAttributeName);

                    if (!existingArrayContainerElement.ContainsKey(existingArrayKey))
                        throw new Exception("The array property, where you are trying to insert the new array, is not a valid property of the primary object. You have to serialize that array first");

                    var existingArray = (List<dynamic>)existingArrayContainerElement[existingArrayKey];

                    if (!existingArray.Any(x => x[insertIntoArrayComparerKey].ToString() == serializedRow.Values.First().ToString()))
                        throw new Exception("There is no element in the existing array that matches with the key from the serialized array");

                    newArrayContainerElement = existingArray.First(x => x[insertIntoArrayComparerKey].ToString() == serializedRow.Values.First().ToString());
                    serializedRow = serializedRow.Skip(1).ToDictionary(x => x.Key, x => x.Value);
                }
                else
                {
                    newArrayContainerElement = GetLeafContainerElement(_object, attributeName);
                }

                if (!newArrayContainerElement.ContainsKey(newArrayKey))
                    newArrayContainerElement.Add(newArrayKey, new List<object>());

                ((List<object>)newArrayContainerElement[newArrayKey]).Add(serializedRow);
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

        private static Dictionary<string, object> SerializeXml(string data)
        {
            if (String.IsNullOrEmpty(data))
                return null;

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

        /// <summary>
        /// Takes the closest element that contains the leaf property
        /// </summary>
        private static Dictionary<string, object> GetLeafContainerElement(Dictionary<string, object> _object, string property)
        {
            var index = property.IndexOf('.');
            if (index != -1)
                return GetLeafContainerElement((Dictionary<string, object>)_object[property.Substring(0, index)], property.Substring(index + 1));
            else
                return _object;
        }

        /// <summary>
        /// Takes the leaf name
        /// </summary>
        private static string GetLeafName(string property)
        {
            var index = property.LastIndexOf('.');
            if (index != -1)
                return property.Substring(index + 1);
            else
                return property;
        }
    }
}