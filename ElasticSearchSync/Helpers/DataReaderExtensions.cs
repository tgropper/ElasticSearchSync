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
                results[r.Values.First()] = r;
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
            InsertIntoArrayComparerKey insertIntoArrayComparerKey = null)
        {
            var arrayElements = new Dictionary<object, List<Dictionary<string, object>>>();
            var cols = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
                cols.Add(reader.GetName(i));

            while (reader.Read())
            {
                var r = SerializeRow(cols, reader, xmlFields);
                var key = r.Values.First();
                if (!arrayElements.ContainsKey(key))
                    arrayElements.Add(key, new List<Dictionary<string, object>>());

                r = r.Skip(1).ToDictionary(x => x.Key, x => x.Value);
                ((List<Dictionary<string, object>>)arrayElements[key]).Add(r);
            }

            //var arrayElements = reader.Serialize(xmlFields)
            //        .Select(x => x.Value)
            //        .GroupBy(x => x.Values.First())
            //        .ToDictionary(x => x.Key, x => x.Select(y => y.Skip(1).ToDictionary(w => w.Key, w => w.Value)).ToList());
            foreach (var @object in results)
            {
                if (arrayElements.ContainsKey(@object.Key))
                {
                    AddArray(@object.Value, arrayElements[@object.Key], attributeName, insertIntoArrayComparerKey);
                }
            }

            return results;
        }

        private static void AddArray(
           Dictionary<string, object> @object,
           IEnumerable<Dictionary<string, object>> arrayElements,
           string fieldName,
           InsertIntoArrayComparerKey insertIntoArrayComparerKey = null)
        {
            var newArrayKey = GetLeafName(fieldName);
            Dictionary<string, object> newArrayContainerElement = null;
            if (insertIntoArrayComparerKey != null)
            {
                var existingArrayAttributeName = fieldName.Substring(0, fieldName.Count() - newArrayKey.Count() - 1);
                var existingArrayContainerElement = GetLeafContainerElement(@object, existingArrayAttributeName);
                var existingArrayKey = GetLeafName(existingArrayAttributeName);

                if (!existingArrayContainerElement.ContainsKey(existingArrayKey))
                    throw new Exception("The array property, where you are trying to insert the new array, is not a valid property of the primary object. You have to serialize that array first");

                var existingArray = (List<Dictionary<string, dynamic>>)existingArrayContainerElement[existingArrayKey];

                var arrayElementsGroupedByArray = arrayElements
                    .GroupBy(x => x[insertIntoArrayComparerKey.NewElementComparerKey])
                    .ToDictionary(x => x.Key, x => x.Select(y => y.Where(z => z.Key != insertIntoArrayComparerKey.NewElementComparerKey).ToDictionary(z => z.Key, z => z.Value)));

                foreach (var arrayElementsByArray in arrayElementsGroupedByArray)
                {
                    var arrayToInsert = arrayElementsByArray.Value;

                    if (!existingArray.Any(x => x[insertIntoArrayComparerKey.ExistingArrayComparerKey].ToString() == arrayElementsByArray.Key.ToString()))
                        throw new Exception("There is no element in the existing array that matches with the key from the serialized array");

                    newArrayContainerElement = existingArray.First(x => x[insertIntoArrayComparerKey.ExistingArrayComparerKey].ToString() == arrayElementsByArray.Key.ToString());

                    if (!newArrayContainerElement.ContainsKey(newArrayKey))
                        newArrayContainerElement.Add(newArrayKey, new Dictionary<string, object>());

                    newArrayContainerElement[newArrayKey] = arrayToInsert;
                }
            }
            else
            {
                newArrayContainerElement = GetLeafContainerElement(@object, fieldName);
                newArrayContainerElement[newArrayKey] = arrayElements;
            }
        }

        /// <summary>
        /// Serialize an object and insert it into the specified primary object
        /// </summary>
        public static Dictionary<object, Dictionary<string, object>> SerializeObject(
            this SqlDataReader reader,
            Dictionary<object, Dictionary<string, object>> results,
            string attributeName,
            InsertIntoArrayComparerKey insertIntoArrayComparerKey = null)
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

            foreach (var @object in results)
            {
                if (serializationResults.ContainsKey(@object.Key))
                {
                    var serializedNewObject = serializationResults[@object.Key];

                    var newObjectKey = GetLeafName(attributeName);
                    Dictionary<string, object> newObjectContainerElement = null;

                    if (insertIntoArrayComparerKey != null)
                    {
                        var existingArrayAttributeName = attributeName.Substring(0, attributeName.Count() - newObjectKey.Count() - 1);
                        var existingArrayContainerElement = GetLeafContainerElement(@object.Value, existingArrayAttributeName);
                        var existingArrayKey = GetLeafName(existingArrayAttributeName);

                        if (!existingArrayContainerElement.ContainsKey(existingArrayKey))
                            throw new Exception("The array property, where you are trying to insert the new array, is not a valid property of the primary object. You have to serialize that array first");

                        var existingArray = (List<Dictionary<string, dynamic>>)existingArrayContainerElement[existingArrayKey];

                        var serializedNewObjectByArray = serializedNewObject
                            .GroupBy(x => x[insertIntoArrayComparerKey.NewElementComparerKey])
                            .ToDictionary(x => x.Key, x => x.ToList());

                        foreach (var serializedNewObjectInArray in serializedNewObjectByArray)
                        {
                            var objectToInsert = SerializeObjectFields(serializedNewObjectInArray.Value);

                            if (!existingArray.Any(x => x[insertIntoArrayComparerKey.ExistingArrayComparerKey].ToString() == serializedNewObjectInArray.Key.ToString()))
                                throw new Exception("There is no element in the existing array that matches with the key from the serialized dynamic object");

                            newObjectContainerElement = existingArray.First(x => x[insertIntoArrayComparerKey.ExistingArrayComparerKey].ToString() == serializedNewObjectInArray.Key.ToString());

                            if (!newObjectContainerElement.ContainsKey(newObjectKey))
                                newObjectContainerElement.Add(newObjectKey, new Dictionary<string, object>());
                            newObjectContainerElement[newObjectKey] = objectToInsert;
                        }
                    }
                    else
                    {
                        newObjectContainerElement = GetLeafContainerElement(@object.Value, attributeName);
                        newObjectContainerElement[newObjectKey] = SerializeObjectFields(serializedNewObject);
                    }
                }
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