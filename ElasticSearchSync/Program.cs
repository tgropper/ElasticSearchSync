using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
//                SqlCommand cmd = new SqlCommand(@"
//                    SELECT TOP 10 rat.ID AS 'ID', 
//			  net.ID AS 'Network.ID', 
//			  net.Description AS 'Network.Description',
//			  country.Description AS 'Network.Country.Description',
//			  country.IsoCode AS 'Network.Country.IsoCode',
//              region.Description AS 'Network.Country.Region.Description',
//			  rat.RatingPerc AS 'RatingPerc'
//FROM dbo.ChannelRating rat 
//JOIN dbo.ChannelNetwork net ON rat.NetworkID = net.ID 
//JOIN dbo.Country country ON net.CountryID = country.ID
//JOIN dbo.Region region ON country.RegionID = region.ID
//WHERE rat.RatingPerc <> 0"
//                                    , conn);

//                SqlCommand cmd = new SqlCommand(@"
//                    SELECT TOP 10 net.ID AS '_id', net.Description AS 'Description', rat.RatingPerc AS 'Rating.Perc'
//FROM dbo.ChannelNetwork net
//JOIN dbo.ChannelRating rat ON rat.NetworkID = net.ID
//WHERE rat.RatingPerc <> 0"
//                    , conn);

                SqlCommand cmd = new SqlCommand(@"
                    SELECT TOP 10 mg.ID AS '_id', 
			  mg.Description AS 'Description', 
			  nw.Description AS 'Network.Description', 
			  rt.RatingPerc AS 'Rating.Perc'
FROM dbo.ChannelMediaGroup mg
JOIN dbo.ChannelNetwork nw ON mg.ID = nw.MediaGroupID
JOIN dbo.ChannelRating rt ON mg.ID = rt.MediaGroupID
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
            for (var i = 0; i < reader.FieldCount; i++)
                cols.Add(reader.GetName(i));

            while (reader.Read())
            {
                var r = SerializeRow(cols, reader, results);
                results.Add(r);
            }

            return results;
        }

        private static Dictionary<string, object> SerializeRow(IEnumerable<string> cols, SqlDataReader reader, IEnumerable<Dictionary<string,object>> results)
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
