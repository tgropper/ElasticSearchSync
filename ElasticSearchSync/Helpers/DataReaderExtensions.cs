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

        /// <summary>
        /// Serialize an object and insert it into the specified primary object
        /// </summary>
        public static Dictionary<object, Dictionary<string, object>> SerializeObject(
            this SqlDataReader reader,
            Dictionary<object, Dictionary<string, object>> results,
            string attributeName,
            string insertIntoArrayComparerKey = null)
        {
            var cols = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
                cols.Add(reader.GetName(i));

            var serializationResults = new Dictionary<object, List<Dictionary<string, object>>>();
            while (reader.Read())
            {
                var serializedRow = SerializeRow(cols, reader);
                var serializationKey = serializedRow.Values.First();
                serializedRow = serializedRow.Skip(1).ToDictionary(x => x.Key, x => x.Value);

                if (!serializationResults.ContainsKey(serializationKey))
                    serializationResults.Add(serializationKey, new List<Dictionary<string, object>>());

                serializationResults[serializationKey].Add(serializedRow);
            }

            foreach (var serializedNewObject in serializationResults)
            {
                if (!results.ContainsKey(serializedNewObject.Key))
                    throw new Exception(String.Format("Object element is related with an object with _id {0}, but it doesn't belong to serialized objects list", serializedNewObject.Key));

                var _object = results[serializedNewObject.Key];

                var objectToInsert = SerializeObjectFields(serializedNewObject.Value);

                var newObjectKey = GetLeafName(attributeName);
                Dictionary<string, object> newObjectContainerElement = null;

                if (insertIntoArrayComparerKey != null)
                {
                    var existingArrayAttributeName = attributeName.Substring(0, attributeName.Count() - newObjectKey.Count() - 1);
                    var existingArrayContainerElement = GetLeafContainerElement(_object, existingArrayAttributeName);
                    var existingArrayKey = GetLeafName(existingArrayAttributeName);

                    if (!existingArrayContainerElement.ContainsKey(existingArrayKey))
                        throw new Exception("The array property, where you are trying to insert the new array, is not a valid property of the primary object. You have to serialize that array first");

                    var existingArray = (List<dynamic>)existingArrayContainerElement[existingArrayKey];

                    var existingArrayElementId = serializedNewObject.Value.First()[insertIntoArrayComparerKey].ToString();
                    if (!existingArray.Any(x => x[insertIntoArrayComparerKey].ToString() == existingArrayElementId))
                        throw new Exception("There is no element in the existing array that matches with the key from the serialized array");

                    newObjectContainerElement = existingArray.First(x => x[insertIntoArrayComparerKey].ToString() == existingArrayElementId);
                }
                else
                {
                    newObjectContainerElement = GetLeafContainerElement(_object, attributeName);
                }

                if (!newObjectContainerElement.ContainsKey(newObjectKey))
                    newObjectContainerElement.Add(newObjectKey, new Dictionary<string, object>());

                newObjectContainerElement[newObjectKey] = objectToInsert;
            }

            return results;
        }

        private static Dictionary<string, object> SerializeObjectFields(List<Dictionary<string, object>> fields)
        {
            var serializedObject = new Dictionary<string, object>();
            foreach (var field in fields)
            {
                SerializeObjectField(field["label"].ToString(), field["value"], serializedObject);
            }

            return serializedObject;
        }

        private static Dictionary<string, object> SerializeObjectField(string label, object value, Dictionary<string, object> serializedObject)
        {
            var objIndex = label.IndexOf('.');
            if (objIndex != -1)
            {
                var objName = label.Substring(0, objIndex);
                if (!serializedObject.ContainsKey(objName))
                    serializedObject.Add(objName, new Dictionary<string, object>());

                SerializeObjectField(
                    label.Substring(objIndex + 1),
                    value,
                    (Dictionary<string, object>)serializedObject[objName]);
            }
            else
            {
                serializedObject[label] = value;
            }
            return serializedObject;
        }

        private static Dictionary<string, object> SerializeRow(
            IEnumerable<string> cols,
            SqlDataReader reader,
            string[] xmlFields = null)
        {
            var result = new Dictionary<string, object>();
            foreach (var col in cols)
            {
                SerializeElement(col, col, reader[col], result, xmlFields);
            }
            return result;
        }

        private static Dictionary<string, object> SerializeElement(
            string col,
            string fullColName,
            object value,
            Dictionary<string, object> result,
            string[] xmlFields = null)
        {
            var objIndex = col.IndexOf('.');
            if (objIndex != -1)
            {
                var objName = col.Substring(0, objIndex);
                if (!result.ContainsKey(objName))
                    result.Add(objName, new Dictionary<string, object>());

                SerializeElement(
                    col.Substring(objIndex + 1),
                    fullColName,
                    value,
                    (Dictionary<string, object>)result[objName]);
            }
            else
            {
                var val = value;
                if (xmlFields != null && xmlFields.Contains(fullColName))
                    val = SerializeXml(val.ToString());

                result[col] = val;
            }
            return result;
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
        /// Returns the container element of the property leaf
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
        /// Returns the property leaf name
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