using System.Data.SqlClient;

namespace ElasticSearchSync
{
    /// <summary>
    /// It represents an object from within the primary object, that its fields are built based on rows and not on columns
    /// </summary>
    /// <example>
    ///     _id     |   field   |   value
    ///     1       |   "name"  |   "jose"
    ///     1       |   "age"   |   26
    ///     1       |   "email" |   "sarasa@sarasa.com"
    ///
    /// This will build the object:
    /// primaryObject: {
    ///     /primaryObject existing fields/
    ///     /newObject.attributeName/: {
    ///         name: "jose",
    ///         age: 26,
    ///         email: "sarasa@sarasa.com"
    ///     }
    /// }
    ///
    /// NOTE: first column of sql script must be the same column used for document _id
    /// NOTE2: this object can have object inside:
    ///
    ///     _id     |   field                           |   value
    ///     1       |   "hasDogs"                       |   true
    ///     1       |   "address.street"                |   "Av. SerializeObjectFields"
    ///     1       |   "address.number"                |   2605
    ///     1       |   "address.zipCode"               |   1992
    ///     1       |   "address.city.name"             |   "Buenos Aires"
    ///     1       |   "address.city.country.isoCode"  |   "AR"
    ///
    /// So the complex object will be:
    /// primaryObject: {
    ///     /primaryObject existing fields/
    ///     /newObject.attributeName/: {
    ///         hasDogs: true,
    ///         address: {
    ///             street: "Av. SerializeObjectFields",
    ///             number: 2605,
    ///             zipCode: 1992,
    ///             city: {
    ///                 name: "Buenos Aires",
    ///                 country: {
    ///                     isoCode: "AR"
    ///                 }
    ///             }
    ///         }
    ///     }
    /// }
    ///
    /// NOTE3: if InsertIntoArrayComparerKey has value, then you must add a second column to match with the array element taken by this key
    ///
    /// If so far you have this serialized object structure:
    ///
    /// object: {
    ///     id,
    ///     name,
    ///     someArray: [{
    ///         idSomeArray
    ///         nameSomeArray
    ///     }]
    /// }
    ///
    ///     _id     |   /insertIntoArrayComparerKey/    |   field   |   value
    ///     1       |   26                              |   "name"  |   "jose"
    ///     1       |   26                              |   "age"   |   26
    ///     1       |   26                              |   "email" |   "sarasa@sarasa.com"
    /// </example>
    public class SyncObjectConfiguration
    {
        /// <summary>
        /// First column of sql script must be the same column used for document _id
        /// </summary>
        public SqlCommand SqlCommand { get; set; }

        /// <summary>
        /// Relative position where the object is gonna to be added in the serialized object
        /// NOTE: selected relative position can't be a leaf property
        /// </summary>
        /// <example>
        /// serialized object:
        ///     note: {
        ///         title
        ///         body
        ///     }
        ///
        /// attributeName:
        ///     note.author
        ///
        /// final serialized object:
        ///     note: {
        ///         title
        ///         body
        ///         author: {
        ///             {/object to be inserted/}
        ///         }
        ///     }
        /// </example>
        public string AttributeName { get; set; }

        public string ParentIdColumn { get; set; }

        /// <summary>
        /// If it has value, the object will be inserted within an array, matching the value of the second column with the array element taken by this key
        /// </summary>
        public string InsertIntoArrayComparerKey { get; set; }
    }
}