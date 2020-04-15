﻿// The MIT License (MIT)

// Copyright (c) 2015 Ben Abelshausen

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.IO;

namespace GTFS.DB.SQLite
{
    /// <summary>
    /// Represents a database that contains GTFS feeds.
    /// </summary>
    public class SQLiteGTFSFeedDB : IGTFSFeedDB
    {        
        /// <summary>
        /// Holds a connection.
        /// </summary>
        private SQLiteConnection _connection;

        public string ConnectionString { get; }

        public DbConnection Connection { get=> new SQLiteConnection(ConnectionString, true).OpenAndReturn(); }

        public object Tag { get; set; }


        public DbParameter CreateParameter(string name, DbType type)
        {
            return new SQLiteParameter(name, type);
        }

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
            return connStr.Substring(connStr.IndexOf("Data Source=") + ("Data Source=").Length, connStr.IndexOf("Data Source=") + connStr.Substring(connStr.IndexOf("Data Source=") + ("Data Source=").Length).IndexOf(";"));
        }

        /// <summary>
        /// Creates a new db.
        /// </summary>
        public SQLiteGTFSFeedDB()
            : this("Data Source=:memory:;Version=3;New=True;")
        {

        }

        /// <summary>
        /// Creates a new db.
        /// </summary>
        public SQLiteGTFSFeedDB(string connectionString)
        {
            ConnectionString = connectionString;
            _connection = new SQLiteConnection(ConnectionString, true);
            _connection.Open();

            // build database.
            this.RebuildDB();
        }

        /// <summary>
        /// Creates a new db.
        /// </summary>
        public SQLiteGTFSFeedDB(FileInfo dbFile, int defaultTimeout = 10, int version = 3)
        {
            ConnectionString = $"Data Source={dbFile.FullName};DefaultTimeout={defaultTimeout};Version={version};";
            _connection = new SQLiteConnection(ConnectionString);
            _connection.Open();

            // build database.
            this.RebuildDB();
        }

        /// <summary>
        /// Adds a new empty feed.
        /// </summary>
        /// <returns></returns>
        public int AddFeed()
        {
            string sqlInsertNewFeed = "INSERT INTO feed VALUES (1, null, null, null, null, null, null);";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sqlInsertNewFeed;
                command.ExecuteNonQuery();
            }
            return (int)_connection.LastInsertRowId;
        }

        /// <summary>
        /// Adds the given feed.
        /// </summary>
        /// <param name="feed"></param>
        /// <returns></returns>
        public int AddFeed(IGTFSFeed feed)
        {
            int newId = this.AddFeed();
            feed.CopyTo(this.GetFeed(newId));
            SortAllTables();
            return newId;
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
            this.RemoveAll("level", id);
            this.RemoveAll("pathway", id);

            string sql = "DELETE FROM feed WHERE ID = :id"; ;
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new SQLiteParameter(@"id", DbType.Int64));
                command.Parameters[0].Value = id;
                return command.ExecuteNonQuery() > 0;
            }
        }

        /// <summary>
        /// Returns all feeds.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<int> GetFeeds()
        {
            string sql = "SELECT id FROM feed";
            var ids = new List<int>();
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        ids.Add((int)reader.GetInt64(0));
                    }                    
                }
            }
            return ids;
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
                command.Parameters.Add(new SQLiteParameter("id", DbType.Int64));
                command.Parameters[0].Value = id;

                using (var reader = command.ExecuteReader())
                { // ok, feed was found!
                    while (reader.Read())
                    {
                        return new SQLiteGTFSFeed(_connection, id);
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// Rebuilds the database.
        /// </summary>
        private void RebuildDB()
        {
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [feed] ( [ID] INTEGER NOT NULL PRIMARY KEY, [feed_publisher_name] TEXT, [feed_publisher_url] TEXT, [feed_lang] TEXT,  [feed_start_date] TEXT, [feed_end_date] TEXT, [feed_version] TEXT );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [agency] ( [FEED_ID] INTEGER NOT NULL, [id] TEXT NOT NULL, [agency_name] TEXT, [agency_url] TEXT, [agency_timezone], [agency_lang] TEXT, [agency_phone] TEXT, [agency_fare_url] TEXT, [agency_email] TEXT );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [calendar] ( [FEED_ID] INTEGER NOT NULL, [service_id] TEXT NOT NULL, [monday] INTEGER, [tuesday] INTEGER, [wednesday] INTEGER, [thursday] INTEGER, [friday] INTEGER, [saturday] INTEGER, [sunday] INTEGER, [start_date] INTEGER, [end_date] INTEGER );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [calendar_date] ( [FEED_ID] INTEGER NOT NULL, [service_id] TEXT NOT NULL, [date] INTEGER, [exception_type] INTEGER );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [fare_attribute] ( [FEED_ID] INTEGER NOT NULL, [fare_id] TEXT NOT NULL, [price] TEXT, [currency_type] TEXT, [payment_method] INTEGER, [transfers] INTEGER, [transfer_duration] TEXT, [agency_id] TEXT );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [fare_rule] ( [FEED_ID] INTEGER NOT NULL, [fare_id] TEXT NOT NULL, [route_id] TEXT NOT NULL, [origin_id] TEXT, [destination_id] TEXT, [contains_id] TEXT );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [frequency] ( [FEED_ID] INTEGER NOT NULL, [trip_id] TEXT NOT NULL, [start_time] TEXT, [end_time] TEXT, [headway_secs] TEXT, [exact_times] INTEGER );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [route] ( [FEED_ID] INTEGER NOT NULL, [id] TEXT NOT NULL, [agency_id] TEXT, [route_short_name] TEXT, [route_long_name] TEXT, [route_desc] TEXT, [route_type] INTEGER NOT NULL, [route_url] TEXT, [route_color] INTEGER, [route_text_color] INTEGER, [vehicle_capacity] INTEGER );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [shape] ( [FEED_ID] INTEGER NOT NULL, [id] TEXT NOT NULL, [shape_pt_lat] REAL, [shape_pt_lon] REAL, [shape_pt_sequence] INTEGER, [shape_dist_traveled] REAL );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [stop] ( [FEED_ID] INTEGER NOT NULL, [id] TEXT NOT NULL, [stop_code] TEXT, [stop_name] TEXT, [stop_desc] TEXT, [stop_lat] REAL, [stop_lon] REAL, [zone_id] TEXT, [stop_url] TEXT, [location_type] INTEGER, [parent_station] TEXT, [stop_timezone] TEXT, [wheelchair_boarding] TEXT, [level_id] TEXT, [platform_code] TEXT);");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [stop_time] ( [FEED_ID] INTEGER NOT NULL, [trip_id] TEXT NOT NULL, [arrival_time] INTEGER, [departure_time] INTEGER, [stop_id] TEXT, [stop_sequence] INTEGER, [stop_headsign] TEXT, [pickup_type] INTEGER, [drop_off_type] INTEGER, [shape_dist_traveled] TEXT, [passenger_boarding] INTEGER, [passenger_alighting] INTEGER, [through_passengers] INTEGER, [total_passengers] INTEGER);");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [transfer] ( [FEED_ID] INTEGER NOT NULL, [from_stop_id] TEXT, [to_stop_id] TEXT, [transfer_type] INTEGER, [min_transfer_time] TEXT );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [trip] ( [FEED_ID] INTEGER NOT NULL, [id] TEXT NOT NULL, [route_id] TEXT, [service_id] TEXT, [trip_headsign] TEXT, [trip_short_name] TEXT, [direction_id] INTEGER, [block_id] TEXT, [shape_id] TEXT, [wheelchair_accessible] INTEGER );");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [pathway] ( [FEED_ID] INTEGER NOT NULL, [pathway_id] TEXT NOT NULL, [from_stop_id] TEXT NOT NULL, [to_stop_id] TEXT NOT NULL, [pathway_mode] INTEGER NOT NULL, [is_bidirectional] INTEGER NOT NULL, [length] REAL, [traversal_time] INTEGER, [stair_count] INTEGER, [max_slope] REAL, [min_width] REAL, [signposted_as] TEXT, [reversed_signposted_as] TEXT);");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [level] ( [FEED_ID] INTEGER NOT NULL, [level_id] TEXT NOT NULL, [level_index] REAL, [level_name] TEXT);");
            // CREATE TABLE TO STORE RESERVED IDS
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [reservedids] ( [base_name] TEXT, [reserved_id] TEXT);");
            // CREATE TABLE TO STORE PREFERENCES
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [preferences] ( [preference] TEXT, [value] TEXT);");
            // CREATE TABLE TO STORE GPX FILENAMES AND TABLE TO STORE CLEANED STOP IDS
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [gpx_filenames] ( [route_id] TEXT, [gpx_filename] TEXT, [track_version] INTEGER);");
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [cleaned_stops] ( [stop_id] TEXT);");
            // CREATE TABLE TO STORE LOGS OF EDITS
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [log] ( [timestamp] TEXT, [action] TEXT, [route_id] TEXT, [pc_name] TEXT, [pc_ip] TEXT, [note] TEXT);");
            // CREATE TABLE TO STORE POLYGONS
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [polygons] ( [id] TEXT NOT NULL, [poly_pt_lat] REAL, [poly_pt_lon] REAL, [poly_pt_seq] INTEGER );");
            // CREATE DATABASE INDEXES FOR EFFICIENT LOOKUP
            this.ExecuteNonQuery("CREATE INDEX IF NOT EXISTS stop_idx ON stop (id)");
            this.ExecuteNonQuery("CREATE INDEX IF NOT EXISTS shape_idx ON shape (id)");
            this.ExecuteNonQuery("CREATE INDEX IF NOT EXISTS stoptimes_idx ON stop_time (trip_id)");


            // Alter existing tables, if their structure is incorrect, for backwards compatibility
            //  1. add agency_email column to agency
            if (!ColumnExists("agency", "agency_email"))
            {
                this.ExecuteNonQuery("ALTER TABLE [agency] ADD [agency_email] TEXT;");
                //if (!ColumnExists("agency", "agency_email"))
                //{
                //    throw new Exception("Failed to create column 'agency_email' in table 'agency'");
                //}
            }
            //  2.1 add passenger_boarding column to stop_time
            if (!ColumnExists("stop_time", "passenger_boarding"))
            {
                this.ExecuteNonQuery("ALTER TABLE [stop_time] ADD [passenger_boarding] INTEGER;");
                //if (!ColumnExists("stop_time", "passenger_boarding"))
                //{
                //    throw new Exception("Failed to create column 'passenger_boarding' in table 'stop_time'");
                //}
            }
            //  2.2 add passenger_alighting column to stop_time
            if (!ColumnExists("stop_time", "passenger_alighting"))
            {
                this.ExecuteNonQuery("ALTER TABLE [stop_time] ADD [passenger_alighting] INTEGER;");
                //if (!ColumnExists("stop_time", "passenger_alighting"))
                //{
                //    throw new Exception("Failed to create column 'passenger_alighting' in table 'stop_time'");
                //}
            }
            //  2.3 add through_passengers column to stop_time
            if (!ColumnExists("stop_time", "through_passengers"))
            {
                this.ExecuteNonQuery("ALTER TABLE [stop_time] ADD [through_passengers] INTEGER;");
                //if (!ColumnExists("stop_time", "through_passengers"))
                //{
                //    throw new Exception("Failed to create column 'through_passengers' in table 'stop_time'");
                //}
            }
            //  2.4 add total_passengers column to stop_time
            if (!ColumnExists("stop_time", "total_passengers"))
            {
                this.ExecuteNonQuery("ALTER TABLE [stop_time] ADD [total_passengers] INTEGER;");
                //if (!ColumnExists("stop_time", "total_passengers"))
                //{
                //    throw new Exception("Failed to create column 'total_passengers' in table 'stop_time'");
                //}
            }
            //  3. add agency_id column to fare_attribute
            if (!ColumnExists("fare_attribute", "agency_id"))
            {
                this.ExecuteNonQuery("ALTER TABLE [fare_attribute] ADD [agency_id] TEXT;");
                //if (!ColumnExists("fare_attribute", "agency_id"))
                //{
                //    throw new Exception("Failed to create column 'agency_id' in table 'fare_attribute'");
                //}
            }
            //  4. add vehicle_capacity column to route
            if (!ColumnExists("route", "vehicle_capacity"))
            {
                this.ExecuteNonQuery("ALTER TABLE [route] ADD [vehicle_capacity] INTEGER;");
                //if (!ColumnExists("route", "vehilcle_capacity"))
                //{
                //    throw new Exception("Failed to create column 'vehicle_capacity' in table 'route'");
                //}
            }
            // 5. add track_version column to gpx_filenames
            if (!ColumnExists("gpx_filenames", "track_version"))
            {
                this.ExecuteNonQuery("ALTER TABLE [gpx_filenames] ADD [track_version] INTEGER;");
            }
            //  6.1 add level_id to stop
            if (!ColumnExists("stop", "level_id"))
            {
                this.ExecuteNonQuery("ALTER TABLE [stop] ADD [level_id] TEXT;");
            }
            //  6.2 add platform_code to stop
            if (!ColumnExists("stop", "platform_code"))
            {
                this.ExecuteNonQuery("ALTER TABLE [stop] ADD [platform_code] TEXT;");
            }
        }

        /// <summary>
        /// Checks if a table with the given name exists in this database.
        /// </summary>
        /// <param name="tableName">The table in this database to look for.</param>
        /// <returns>True if th</returns>
        public bool TableExists(string tableName)
        {
            using (var command = new SQLiteCommand($"SELECT name FROM sqlite_master WHERE type='table' AND name='{tableName}';", _connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var value = reader.GetValue(0);
                        if (tableName.Equals(value))
                        {
                            reader.Close();
                            return true;
                        }
                    }
                    reader.Close();
                    return false;
                }
            }
        }

        /// <summary>
        /// Checks if the given table contains a column with the given name.
        /// </summary>
        /// <param name="tableName">The table in this database to check.</param>
        /// <param name="columnName">The column in the given table to look for.</param>
        /// <returns>True if the given table contains a column with the given name.</returns>
        public bool ColumnExists(string tableName, string columnName)
        {
            using (var command = new SQLiteCommand("PRAGMA table_info(" + tableName + ")", _connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var value = reader.GetValue(1);//column 1 from the result contains the column names
                        if (columnName.Equals(value))
                        {
                            reader.Close();
                            return true;
                        }
                    }

                    reader.Close();
                    return false;
                }
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
                command.Parameters.Add(new SQLiteParameter(@"feed_id", DbType.Int64));
                command.Parameters[0].Value = id;
                command.ExecuteNonQuery();
            }
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

        public void CloseConnection()
        {
            _connection.Close();
        }

        /// <summary>
        /// Deletes and recreates the routes, trips, stops, stop_times, frequencies and calendar_dates tables in a sorted order - may take some time
        /// </summary>
        public void SortAllTables()
        {
            SortRoutes();
            SortTrips();
            SortStops();
            SortStopTimes();
            SortFrequencies();
            SortCalendars();
            SortCalendarDates();
            SortShapes();
            SortPolygons();
        }

        /// <summary>
        /// Deletes and recreates the routes table in a sorted order - may take time
        /// </summary>
        public void SortRoutes()
        {
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [route_sorted] ( [FEED_ID] INTEGER NOT NULL, [id] TEXT NOT NULL, [agency_id] TEXT, [route_short_name] TEXT, [route_long_name] TEXT, [route_desc] TEXT, [route_type] INTEGER NOT NULL, [route_url] TEXT, [route_color] INTEGER, [route_text_color] INTEGER, [vehicle_capacity] INTEGER );");
            this.ExecuteNonQuery("INSERT INTO route_sorted (FEED_ID, id, agency_id, route_short_name, route_long_name, route_desc, route_type, route_url, route_color, route_text_color, vehicle_capacity) SELECT FEED_ID, id, agency_id, route_short_name, route_long_name, route_desc, route_type, route_url, route_color, route_text_color, vehicle_capacity FROM route ORDER BY id ASC;");
            this.ExecuteNonQuery("DROP TABLE route");
            this.ExecuteNonQuery("ALTER TABLE route_sorted RENAME TO route");
        }

        /// <summary>
        /// Deletes and recreates the trips table in a sorted order - may take time
        /// </summary>
        public void SortTrips()
        {
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [trip_sorted] ( [FEED_ID] INTEGER NOT NULL, [id] TEXT NOT NULL, [route_id] TEXT, [service_id] TEXT, [trip_headsign] TEXT, [trip_short_name] TEXT, [direction_id] INTEGER, [block_id] TEXT, [shape_id] TEXT, [wheelchair_accessible] INTEGER );");
            this.ExecuteNonQuery("INSERT INTO trip_sorted (FEED_ID, id, route_id, service_id, trip_headsign, trip_short_name, direction_id, block_id, shape_id, wheelchair_accessible) SELECT FEED_ID, id, route_id, service_id, trip_headsign, trip_short_name, direction_id, block_id, shape_id, wheelchair_accessible FROM trip ORDER BY id ASC;");
            this.ExecuteNonQuery("DROP TABLE trip");
            this.ExecuteNonQuery("ALTER TABLE trip_sorted RENAME TO trip");
        }

        /// <summary>
        /// Deletes and recreates the stops table in a sorted order - may take time
        /// </summary>
        public void SortStops()
        {
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [stop_sorted] ( [FEED_ID] INTEGER NOT NULL, [id] TEXT NOT NULL, [stop_code] TEXT, [stop_name] TEXT, [stop_desc] TEXT, [stop_lat] REAL, [stop_lon] REAL, [zone_id] TEXT, [stop_url] TEXT, [location_type] INTEGER, [parent_station] TEXT, [stop_timezone] TEXT, [wheelchair_boarding] TEXT, [level_id] TEXT, [platform_code] TEXT);");
            this.ExecuteNonQuery("INSERT INTO stop_sorted (FEED_ID, id, stop_code, stop_name, stop_desc, stop_lat, stop_lon, zone_id, stop_url, location_type, parent_station, stop_timezone, wheelchair_boarding, level_id, platform_code) SELECT FEED_ID, id, stop_code, stop_name, stop_desc, stop_lat, stop_lon, zone_id, stop_url, location_type, parent_station, stop_timezone, wheelchair_boarding, level_id, platform_code FROM stop ORDER BY id ASC;");
            this.ExecuteNonQuery("DROP TABLE stop");
            this.ExecuteNonQuery("ALTER TABLE stop_sorted RENAME TO stop");
        }

        /// <summary>
        /// Deletes and recreates the stop_times table in a sorted order - may take time
        /// </summary>
        public void SortStopTimes()
        {
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [stop_time_sorted] ( [FEED_ID] INTEGER NOT NULL, [trip_id] TEXT NOT NULL, [arrival_time] INTEGER, [departure_time] INTEGER, [stop_id] TEXT, [stop_sequence] INTEGER, [stop_headsign] TEXT, [pickup_type] INTEGER, [drop_off_type] INTEGER, [shape_dist_traveled] TEXT, [passenger_boarding] INTEGER, [passenger_alighting] INTEGER, [through_passengers] INTEGER, [total_passengers] INTEGER );");
            this.ExecuteNonQuery("INSERT INTO stop_time_sorted (FEED_ID, trip_id, arrival_time, departure_time, stop_id, stop_sequence, stop_headsign, pickup_type, drop_off_type, shape_dist_traveled, passenger_boarding, passenger_alighting, through_passengers, total_passengers) SELECT FEED_ID, trip_id, arrival_time, departure_time, stop_id, stop_sequence, stop_headsign, pickup_type, drop_off_type, shape_dist_traveled, passenger_boarding, passenger_alighting, through_passengers, total_passengers FROM stop_time ORDER BY trip_id ASC, stop_sequence ASC;");
            this.ExecuteNonQuery("DROP TABLE stop_time");
            this.ExecuteNonQuery("ALTER TABLE stop_time_sorted RENAME TO stop_time");
        }

        /// <summary>
        /// Deletes and recreates the frequencies table in a sorted order - may take time
        /// </summary>
        public void SortFrequencies()
        {
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [frequency_sorted] ( [FEED_ID] INTEGER NOT NULL, [trip_id] TEXT NOT NULL, [start_time] TEXT, [end_time] TEXT, [headway_secs] TEXT, [exact_times] INTEGER );");
            this.ExecuteNonQuery("INSERT INTO frequency_sorted (FEED_ID, trip_id, start_time, end_time, headway_secs, exact_times) SELECT FEED_ID, trip_id, start_time, end_time, headway_secs, exact_times FROM frequency ORDER BY trip_id ASC;");
            this.ExecuteNonQuery("DROP TABLE frequency");
            this.ExecuteNonQuery("ALTER TABLE frequency_sorted RENAME TO frequency");
        }

        public void SortCalendars()
        {
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [calendar_sorted] ( [FEED_ID] INTEGER NOT NULL, [service_id] TEXT NOT NULL, [monday] INTEGER, [tuesday] INTEGER, [wednesday] INTEGER, [thursday] INTEGER, [friday] INTEGER, [saturday] INTEGER, [sunday] INTEGER, [start_date] INTEGER, [end_date] INTEGER );");
            this.ExecuteNonQuery("INSERT INTO calendar_sorted (FEED_ID, service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date) SELECT FEED_ID, service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date FROM calendar ORDER BY service_id ASC;");
            this.ExecuteNonQuery("DROP TABLE calendar");
            this.ExecuteNonQuery("ALTER TABLE calendar_sorted RENAME TO calendar");
        }

        /// <summary>
        /// Deletes and recreates the calendar_dates table in a sorted order (first by date then by exception_type) - may take time
        /// </summary>
        public void SortCalendarDates()
        {
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [calendar_date_sorted] ( [FEED_ID] INTEGER NOT NULL, [service_id] TEXT NOT NULL, [date] INTEGER, [exception_type] INTEGER );");
            this.ExecuteNonQuery("INSERT INTO calendar_date_sorted (FEED_ID, service_id, date, exception_type) SELECT FEED_ID, service_id, date, exception_type FROM calendar_date ORDER BY date, exception_type ASC;");
            this.ExecuteNonQuery("DROP TABLE calendar_date");
            this.ExecuteNonQuery("ALTER TABLE calendar_date_sorted RENAME TO calendar_date");
        }

        /// <summary>
        /// Deletes and recreates the shapes table in a sorted order (first by id then by sequence) - will take time
        /// </summary>
        public void SortShapes()
        {
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [shape_sorted] ( [FEED_ID] INTEGER NOT NULL, [id] TEXT NOT NULL, [shape_pt_lat] REAL, [shape_pt_lon] REAL, [shape_pt_sequence] INTEGER, [shape_dist_traveled] REAL );");
            this.ExecuteNonQuery("INSERT INTO shape_sorted (FEED_ID, id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled) SELECT FEED_ID, id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled FROM shape ORDER BY id, shape_pt_sequence ASC;");
            this.ExecuteNonQuery("DROP TABLE shape");
            this.ExecuteNonQuery("ALTER TABLE shape_sorted RENAME TO shape");
        }

        /// <summary>
        /// Deletes and recreates the polygons table in a sorted order (first by id then by poly_pt_seq) - may take time
        /// </summary>
        public void SortPolygons()
        {
            this.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS [polygons_sorted] ( [id] TEXT NOT NULL, [poly_pt_lat] REAL, [poly_pt_lon] REAL, [poly_pt_seq] INTEGER );");
            this.ExecuteNonQuery("INSERT INTO polygons_sorted (id, poly_pt_lat, poly_pt_lon, poly_pt_seq) SELECT id, poly_pt_lat, poly_pt_lon, poly_pt_seq FROM polygons ORDER BY id ASC, poly_pt_seq ASC;");
            this.ExecuteNonQuery("DROP TABLE polygons");
            this.ExecuteNonQuery("ALTER TABLE polygons_sorted RENAME TO polygons");
        }
    }
}