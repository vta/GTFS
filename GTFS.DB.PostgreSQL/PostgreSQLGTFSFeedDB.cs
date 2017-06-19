using System;
using Npgsql;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GTFS.DB.PostgreSQL
{
    public class PostgreSQLGTFSFeedDB : IGTFSFeedDB
    {
        /// <summary>
        /// Creates a new db.
        /// </summary>
        public PostgreSQLGTFSFeedDB(string connectionString)
        {
            var connection = new NpgsqlConnection("Server=127.0.0.1;User Id=postgres;Password=x84yay;Database=GTFS");
            connection.Open();
        }

        public int AddFeed()
        {
            throw new NotImplementedException();
        }

        public int AddFeed(IGTFSFeed feed)
        {
            throw new NotImplementedException();
        }

        public IGTFSFeed GetFeed(int id)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<int> GetFeeds()
        {
            throw new NotImplementedException();
        }

        public bool RemoveFeed(int id)
        {
            throw new NotImplementedException();
        }
    }
}
