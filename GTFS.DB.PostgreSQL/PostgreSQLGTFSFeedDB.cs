using System;
using Npgsql;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.Common;

namespace GTFS.DB.PostgreSQL
{
    /// <summary>
    /// Represents a Postgre database that contains GTFS feeds.
    /// </summary>
    public class PostgreSQLGTFSFeedDB : IGTFSFeedDB
    {
        /// <summary>
        /// Holds a connection.
        /// </summary>
        private NpgsqlConnection _connection;

        DbConnection IGTFSFeedDB._connection { get => _connection; }

        /// <summary>
        /// Returns the data source (filename of the db)
        /// </summary>
        public string GetDataSource()
        {
            return _connection.DataSource;
        }

        /// <summary>
        /// Returns the data source in full (location of the db)
        /// </summary>
        public string GetFullDataSource()
        {
            string connStr = _connection.ConnectionString;
            return connStr;
        }

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
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS calendar ( FEED_ID INTEGER NOT NULL, service_id TEXT NOT NULL, monday INTEGER, tuesday INTEGER, wednesday INTEGER, thursday INTEGER, friday INTEGER, saturday INTEGER, sunday INTEGER, start_date BIGINT, end_date BIGINT );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS calendar_date ( FEED_ID INTEGER NOT NULL, service_id TEXT NOT NULL, date BIGINT, exception_type INTEGER );");
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

        /// <summary>
        /// Returns the feed for the given id.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public IGTFSFeed GetFeed(int id)
        {
            string sql = "SELECT id FROM feed WHERE ID = :id";
            var ids = new List<int>();
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter("id", DbType.Int64));
                command.Parameters[0].Value = id;

                using (var reader = command.ExecuteReader())
                { // ok, feed was found!
                    while (reader.Read())
                    {
                        return new PostgreSQLGTFSFeed(_connection, id);
                    }
                }
            }
            return null;
        }

        public IEnumerable<int> GetFeeds()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Removes the given feed.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool RemoveFeed(int id)
        {
            this.RemoveAll("agency", id);
            this.RemoveAll("calendar", id);
            this.RemoveAll("calendar_date", id);
            this.RemoveAll("fare_attribute", id);
            this.RemoveAll("fare_rule", id);
            this.RemoveAll("frequency", id);
            this.RemoveAll("route", id);
            this.RemoveAll("shape", id);
            this.RemoveAll("stop", id);
            this.RemoveAll("stop_time", id);
            this.RemoveAll("transfer", id);
            this.RemoveAll("trip", id);

            string sql = "DELETE FROM feed WHERE ID = :id"; ;
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"id", DbType.Int64));
                command.Parameters[0].Value = id;
                return command.ExecuteNonQuery() > 0;
            }
        }

        /// <summary>
        /// Deletes all info from one table about one feed.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="id"></param>
        private void RemoveAll(string table, int id)
        {
            string sql = string.Format("DELETE FROM {0} WHERE FEED_ID = :feed_id", table);
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters[0].Value = id;
                command.ExecuteNonQuery();
            }
        }
    }
}
