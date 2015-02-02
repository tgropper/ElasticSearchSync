using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace ElasticSearchSync
{
    public class Program
    {
        static void Main(string[] args)
        {

            SqlConnection conn = new SqlConnection("Data Source=(local);Initial Catalog=FOXratings;User Id=sa;Password=1234;");
            SqlDataReader rdr = null;

            try
            {
                conn.Open();

                SqlCommand cmd = new SqlCommand(@"
                    SELECT TOP 10 mg.ID AS '_id', 
              mg.ID AS 'MediaGroup.ID',
			  mg.Description AS 'MediaGroup.Description', 
			  nw.ID AS 'MediaGroup.Network.ID',
			  nw.Description AS 'MediaGroup.Network.Description',
			  ct.IsoCode AS 'MediaGroup.Network.Country.IsoCode',
			  rt.RatingPerc AS 'MediaGroup.Rating.Perc',
              tg.Description AS 'MediaGroup.Rating.Target.Description'
FROM dbo.ChannelMediaGroup mg
JOIN dbo.ChannelNetwork nw ON mg.ID = nw.MediaGroupID
JOIN dbo.ChannelRating rt ON mg.ID = rt.MediaGroupID
JOIN dbo.ChannelTarget tg ON tg.ID = rt.TargetID
JOIN dbo.Country ct ON ct.ID = nw.CountryID

WHERE rt.RatingPerc <> 0"
                    , conn);

                rdr = cmd.ExecuteReader();
                var data = Helper.Serialize(rdr);
                var json = JsonConvert.SerializeObject(data, Formatting.Indented);

                Console.WriteLine(json);

                Console.ReadLine();
            }
            finally
            {
                if (rdr != null)
                    rdr.Close();

                if (conn != null)
                    conn.Close();
            }
        }
    }

    public static class Helper 
    {
        public static IEnumerable<Dictionary<string, object>> Serialize(this SqlDataReader reader)
        {
            var results = new List<Dictionary<string, object>>();
            var cols = new List<string>();
            var arrayCols = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                var name = reader.GetName(i);
                cols.Add(name);
            }

            while (reader.Read())
            {
                var result = SerializeRow(cols, reader);
                results.Add(result);
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
    }
}
