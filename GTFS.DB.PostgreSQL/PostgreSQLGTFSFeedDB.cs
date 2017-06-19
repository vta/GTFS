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
        /// Holds a connection.
        /// </summary>
        public NpgsqlConnection _connection;

        /// <summary>
        /// Creates a new db.
        /// </summary>
        public PostgreSQLGTFSFeedDB(string connectionString)
        {
            _connection = new NpgsqlConnection(connectionString);
            _connection.Open();

            // build database.
            this.RebuildDB();
        }

        /// <summary>
        /// Rebuilds the database.
        /// </summary>
        private void RebuildDB()
        {
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS feed ( ID INTEGER NOT NULL PRIMARY KEY, feed_publisher_name TEXT, feed_publisher_url TEXT, feed_lang TEXT,  feed_start_date TEXT, feed_end_date TEXT, feed_version TEXT );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS agency ( FEED_ID INTEGER NOT NULL, id TEXT NOT NULL, agency_name TEXT, agency_url TEXT, agency_timezone TEXT, agency_lang TEXT, agency_phone TEXT, agency_fare_url TEXT );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS calendar ( FEED_ID INTEGER NOT NULL, service_id TEXT NOT NULL, monday INTEGER, tuesday INTEGER, wednesday INTEGER, thursday INTEGER, friday INTEGER, saturday INTEGER, sunday INTEGER, start_date INTEGER, end_date INTEGER );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS calendar_date ( FEED_ID INTEGER NOT NULL, service_id TEXT NOT NULL, date INTEGER, exception_type INTEGER );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS fare_attribute ( FEED_ID INTEGER NOT NULL, fare_id TEXT NOT NULL, price TEXT, currency_type TEXT, payment_method INTEGER, transfers INTEGER, transfer_duration TEXT );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS fare_rule ( FEED_ID INTEGER NOT NULL, fare_id TEXT NOT NULL, route_id TEXT NOT NULL, origin_id TEXT, destination_id TEXT, contains_id TEXT );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS frequency ( FEED_ID INTEGER NOT NULL, trip_id TEXT NOT NULL, start_time TEXT, end_time TEXT, headway_secs TEXT, exact_times INTEGER );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS route ( FEED_ID INTEGER NOT NULL, id TEXT NOT NULL, agency_id TEXT, route_short_name TEXT, route_long_name TEXT, route_desc TEXT, route_type INTEGER NOT NULL, route_url TEXT, route_color INTEGER, route_text_color INTEGER );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS shape ( FEED_ID INTEGER NOT NULL, id TEXT NOT NULL, shape_pt_lat REAL, shape_pt_lon REAL, shape_pt_sequence INTEGER, shape_dist_traveled REAL );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS stop ( FEED_ID INTEGER NOT NULL, id TEXT NOT NULL, stop_code TEXT, stop_name TEXT, stop_desc TEXT, stop_lat REAL, stop_lon REAL, zone_id TEXT, stop_url TEXT, location_type INTEGER, parent_station TEXT, stop_timezone TEXT, wheelchair_boarding TEXT );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS stop_time ( FEED_ID INTEGER NOT NULL, trip_id TEXT NOT NULL, arrival_time INTEGER, departure_time INTEGER, stop_id TEXT, stop_sequence INTEGER, stop_headsign TEXT, pickup_type INTEGER, drop_off_type INTEGER, shape_dist_traveled TEXT );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS transfer ( FEED_ID INTEGER NOT NULL, from_stop_id TEXT, to_stop_id TEXT, transfer_type INTEGER, min_transfer_time TEXT );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS trip ( FEED_ID INTEGER NOT NULL, id TEXT NOT NULL, route_id TEXT, service_id TEXT, trip_headsign TEXT, trip_short_name TEXT, direction_id INTEGER, block_id TEXT, shape_id TEXT, wheelchair_accessible INTEGER );");
            // CREATE TABLE TO STORE GPX FILENAMES AND TABLE TO STORE CLEANED STOP IDS
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS gpx_filenames ( route_id TEXT, gpx_filename TEXT);");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS cleaned_stops ( stop_id TEXT);");
            // CREATE TABLE TO STORE LOGS OF EDITS
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS log ( timestamp TEXT, action TEXT, route_id TEXT, pc_name TEXT, pc_ip TEXT, note TEXT);");
            // CREATE DATABASE INDEXES FOR EFFICIENT LOOKUP
            this.ExecuteNonQuery("CREATE INDEX IF NOT EXISTS stop_idx ON stop (id)");
            this.ExecuteNonQuery("CREATE INDEX IF NOT EXISTS shape_idx ON shape (id)");
            this.ExecuteNonQuery("CREATE INDEX IF NOT EXISTS stoptimes_idx ON stop_time (trip_id)");
        }

        /// <summary>
        /// Executes sql on the db.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns>Returns number of rows affected</returns>
        private int ExecuteNonQuery(string sql)
        {
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                return command.ExecuteNonQuery();
            }
        }

        public int AddFeed()
        {
            string sqlInsertNewFeed = "INSERT INTO feed VALUES (1, null, null, null, null, null, null);";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sqlInsertNewFeed;
                command.ExecuteNonQuery();
            }
            return (int)1;
        }

        public int AddFeed(IGTFSFeed feed)
        {
            int newId = this.AddFeed();
            feed.CopyTo(this.GetFeed(newId));
            return newId;
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
