// The MIT License (MIT)

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

using GTFS.DB.PostgreSQL.Collections;
using GTFS.Entities;
using GTFS.Entities.Collections;
using Npgsql;
using System;
using System.Data;
using System.Linq;

namespace GTFS.DB.PostgreSQL
{
    /// <summary>
    /// Represents a GFTS feed using an PostgreSQL database.
    /// </summary>
    internal class PostgreSQLGTFSFeed : IGTFSFeed
    {
        /// <summary>
        /// Holds the connection.
        /// </summary>
        private NpgsqlConnection _connection;

        /// <summary>
        /// Holds the id.
        /// </summary>
        private int _id;

        /// <summary>
        /// Creates a new PostgreSQL GTFS feed.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="id"></param>
        internal PostgreSQLGTFSFeed(NpgsqlConnection connection, int id)
        {
            _connection = connection;
            _id = id;

            this.Agencies = new PostgreSQLAgencyCollection(_connection, id);
            this.CalendarDates = new PostgreSQLCalendarDateCollection(_connection, id);
            this.Calendars = new PostgreSQLCalendarCollection(_connection, id);
            this.FareAttributes = new PostgreSQLFareAttributeCollection(_connection, id);
            this.FareRules = new PostgreSQLFareRuleCollection(_connection, id);
            this.Frequencies = new PostgreSQLFrequencyCollection(_connection, id);
            this.Routes = new PostgreSQLRouteCollection(_connection, id);
            this.Shapes = new PostgreSQLShapeCollection(_connection, id);
            this.Stops = new PostgreSQLStopCollection(_connection, id);
            this.StopTimes = new PostgreSQLStopTimeCollection(_connection, id);
            this.Transfers = new PostgreSQLTransferCollection(_connection, id);
            this.Trips = new PostgreSQLTripCollection(_connection, id);
            this.Levels = new PostgreSQLLevelCollection(_connection, id);
            this.Pathways = new PostgreSQLPathwayCollection(_connection, id);
        }

        /// <summary>
        /// Sets the feed info.
        /// </summary>
        /// <param name="feedInfo"></param>
        public void SetFeedInfo(FeedInfo feedInfo)
        {
            string sql = "UPDATE feed SET feed_publisher_name = :feed_publisher_name, feed_publisher_url = :feed_publisher_url, feed_lang = :feed_lang, feed_start_date = :feed_start_date, feed_end_date = :feed_end_date, feed_version = :feed_version WHERE ID = :id;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_publisher_name", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"feed_publisher_url", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"feed_lang", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"feed_start_date", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"feed_end_date", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"feed_version", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"id", DbType.Int64));

                command.Parameters[0].Value = feedInfo.PublisherName;
                command.Parameters[1].Value = feedInfo.PublisherUrl;
                command.Parameters[2].Value = feedInfo.Lang;
                command.Parameters[3].Value = feedInfo.StartDate;
                command.Parameters[4].Value = feedInfo.EndDate;
                command.Parameters[5].Value = feedInfo.Version;
                command.Parameters[6].Value = _id;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Returns the feed info.
        /// </summary>
        /// <returns></returns>
        public FeedInfo GetFeedInfo()
        {
            FeedInfo feedInfo = null;
            string sql = "SELECT ID, feed_publisher_name, feed_publisher_url, feed_lang, feed_start_date, feed_end_date, feed_version FROM feed WHERE ID = :id;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"id", DbType.Int64));
                command.Parameters[0].Value = _id;

                using(var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        feedInfo = new FeedInfo();
                        feedInfo.PublisherName = reader.IsDBNull(1) ? null : reader.GetString(1);
                        feedInfo.PublisherUrl = reader.IsDBNull(2) ? null : reader.GetString(2);
                        feedInfo.Lang = reader.IsDBNull(3) ? null : reader.GetString(3);
                        feedInfo.StartDate = reader.IsDBNull(4) ? null : reader.GetString(4);
                        feedInfo.EndDate = reader.IsDBNull(5) ? null : reader.GetString(5);
                        feedInfo.Version = reader.IsDBNull(6) ? null : reader.GetString(6);
                        break;
                    }
                }
            }
            return feedInfo;
        }

        /// <summary>
        /// Gets the collection of agencies.
        /// </summary>
        public IUniqueEntityCollection<Agency> Agencies
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the collection of calendars.
        /// </summary>
        public IEntityCollection<Calendar> Calendars
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the collection of calendardates.
        /// </summary>
        public IEntityCollection<CalendarDate> CalendarDates
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the collection of fare attributes.
        /// </summary>
        public IEntityCollection<FareAttribute> FareAttributes
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the collection of fare rules.
        /// </summary>
        public IUniqueEntityCollection<FareRule> FareRules
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the collection of frequencies.
        /// </summary>
        public IEntityCollection<Frequency> Frequencies
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the collection of routes.
        /// </summary>
        public IUniqueEntityCollection<Route> Routes
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the collection of shapes.
        /// </summary>
        public IEntityCollection<Shape> Shapes
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the collection of stops.
        /// </summary>
        public IUniqueEntityCollection<Stop> Stops
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the collection of stop times.
        /// </summary>
        public IStopTimeCollection StopTimes
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the collection of transfers.
        /// </summary>
        public ITransferCollection Transfers
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the collection of trips.
        /// </summary>
        public IUniqueEntityCollection<Trip> Trips
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the collection of levels.
        /// </summary>
        public IUniqueEntityCollection<Level> Levels
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the collection of pathways.
        /// </summary>
        public IUniqueEntityCollection<Pathway> Pathways
        {
            get;
            private set;
        }
    }
}