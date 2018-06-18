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

using GTFS.Entities;
using GTFS.Entities.Collections;
using GTFS.Entities.Enumerations;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace GTFS.DB.PostgreSQL.Collections
{
    /// <summary>
    /// Represents a collection of Trips using an SQLite database.
    /// </summary>
    public class PostgreSQLTripCollection : IUniqueEntityCollection<Trip>
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
        /// Creates a new SQLite GTFS feed.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="id"></param>
        internal PostgreSQLTripCollection(NpgsqlConnection connection, int id)
        {
            _connection = connection;
            _id = id;
        }

        public void Add(Trip trip)
        {
            string sql = "INSERT INTO trip VALUES (:feed_id, :id, :route_id, :service_id, :trip_headsign, :trip_short_name, :direction_id, :block_id, :shape_id, :wheelchair_accessible);";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"route_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"service_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"trip_headsign", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"trip_short_name", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"direction_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"block_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"shape_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"wheelchair_accessible", DbType.Int64));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = trip.Id;
                command.Parameters[2].Value = trip.RouteId;
                command.Parameters[3].Value = trip.ServiceId;
                command.Parameters[4].Value = trip.Headsign;
                command.Parameters[5].Value = trip.ShortName;
                command.Parameters[6].Value = trip.Direction.HasValue ? (int?)trip.Direction.Value : null;
                command.Parameters[7].Value = trip.BlockId;
                command.Parameters[8].Value = trip.ShapeId;
                command.Parameters[9].Value = trip.AccessibilityType.HasValue ? (int?)trip.AccessibilityType.Value : null;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                command.ExecuteNonQuery();
            }
        }

        public void AddRange(IUniqueEntityCollection<Trip> entities)
        {
            using (var writer = _connection.BeginBinaryImport("COPY trip (feed_id, id, route_id, service_id, trip_headsign, trip_short_name, direction_id, block_id, shape_id, wheelchair_accessible) FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var trip in entities)
                {
                    writer.StartRow();
                    writer.Write(_id, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(trip.Id, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(trip.RouteId, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(trip.ServiceId, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(trip.Headsign, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(trip.ShortName, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(trip.Direction, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(trip.BlockId, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(trip.ShapeId, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(trip.AccessibilityType, NpgsqlTypes.NpgsqlDbType.Integer);
                }
            }
        }

        public Trip Get(string entityId)
        {
            string sql = "SELECT id, route_id, service_id, trip_headsign, trip_short_name, direction_id, block_id, shape_id, wheelchair_accessible FROM trip WHERE FEED_ID = :id AND ID = :trip_id;";
            var parameters = new List<NpgsqlParameter>();
            parameters.Add(new NpgsqlParameter(@"id", DbType.Int64));
            parameters.Add(new NpgsqlParameter(@"trip_id", DbType.Int64));
            parameters[0].Value = _id;
            parameters[1].Value = entityId;

            return new PostgreSQLEnumerable<Trip>(_connection, sql, parameters.ToArray(), (x) =>
            {
                return new Trip()
                {
                    Id = x.GetString(0),
                    ServiceId = x.IsDBNull(1) ? null : x.GetString(1),
                    Headsign = x.IsDBNull(2) ? null : x.GetString(2),
                    ShortName = x.IsDBNull(3) ? null : x.GetString(3),
                    Direction = x.IsDBNull(4) ? null : (DirectionType?)x.GetInt64(4),
                    BlockId = x.IsDBNull(5) ? null : x.GetString(5),
                    ShapeId = x.IsDBNull(6) ? null : x.GetString(6),
                    AccessibilityType = x.IsDBNull(7) ? null : (WheelchairAccessibilityType?)x.GetInt64(7)
                };
            }).FirstOrDefault();
        }



        public Trip Get(int idx)
        {
            throw new NotImplementedException();
        }

        public bool Remove(string entityId)
        {
            string sql = "DELETE FROM trip WHERE FEED_ID = :feed_id AND id = :trip_id;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"trip_id", DbType.String));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = entityId;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                return command.ExecuteNonQuery() > 0;
            }
        }

        public void RemoveRange(IEnumerable<string> entityIds)
        {
            using (var command = _connection.CreateCommand())
            {
                using (var transaction = _connection.BeginTransaction())
                {
                    foreach (var tripId in entityIds)
                    {
                        string sql = "DELETE FROM trip WHERE FEED_ID = :feed_id AND id = :trip_id;";
                        command.CommandText = sql;
                        command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                        command.Parameters.Add(new NpgsqlParameter(@"trip_id", DbType.String));

                        command.Parameters[0].Value = _id;
                        command.Parameters[1].Value = tripId;

                        command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                        command.ExecuteNonQuery();
                    }
                    transaction.Commit();
                }
            }
        }

        public IEnumerable<Trip> Get()
        {
            #if DEBUG
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            Console.Write($"Fetching trips...");
            #endif
            var trips = new List<Trip>();
            using (var reader = _connection.BeginBinaryExport("COPY trip TO STDOUT (FORMAT BINARY)"))
            {
                while (reader.StartRow() > 0)
                {
                    var feedId = reader.Read<int>(NpgsqlTypes.NpgsqlDbType.Integer);
                    trips.Add(new Trip()
                    {
                        Id = reader.ReadStringSafe(),
                        RouteId = reader.ReadStringSafe(),
                        ServiceId = reader.ReadStringSafe(),
                        Headsign = reader.ReadStringSafe(),
                        ShortName = reader.ReadStringSafe(),
                        Direction = (DirectionType?)reader.ReadIntSafe(),
                        BlockId = reader.ReadStringSafe(),
                        ShapeId = reader.ReadStringSafe(),
                        AccessibilityType = (WheelchairAccessibilityType?)reader.ReadIntSafe()
                    });
                }
            }
            #if DEBUG
            stopwatch.Stop();
            Console.WriteLine($" {stopwatch.ElapsedMilliseconds} ms");
            #endif
            return trips;
        }

        /// <summary>
        /// Returns entity ids
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetIds()
        {
            #if DEBUG
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            Console.Write($"Fetching trip ids...");
            #endif
            var outList = new List<string>();
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = "SELECT DISTINCT(id) FROM trip";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        outList.Add(Convert.ToString(reader["id"]));
                    }
                }
            }
            #if DEBUG
            stopwatch.Stop();
            Console.WriteLine($" {stopwatch.ElapsedMilliseconds} ms");
            #endif
            return outList;
        }

        public int Count
        {
            get { throw new NotImplementedException(); }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns></returns>
        public IEnumerator<Trip> GetEnumerator()
        {
            return this.Get().GetEnumerator();
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns></returns>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.Get().GetEnumerator();
        }

        public bool Update(string entityId, Trip entity)
        {
            string sql = "UPDATE trip SET FEED_ID=:feed_id, id=:id, route_id=:route_id, service_id=:service_id, trip_headsign=:trip_headsign, trip_short_name=:trip_short_name, direction_id=:direction_id, block_id=:block_id, shape_id=:shape_id, wheelchair_accessible=:wheelchair_accessible WHERE id=:entityId;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"route_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"service_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"trip_headsign", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"trip_short_name", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"direction_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"block_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"shape_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"wheelchair_accessible", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"entityId", DbType.String));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = entity.Id;
                command.Parameters[2].Value = entity.RouteId;
                command.Parameters[3].Value = entity.ServiceId;
                command.Parameters[4].Value = entity.Headsign;
                command.Parameters[5].Value = entity.ShortName;
                command.Parameters[6].Value = entity.Direction.HasValue ? (int?)entity.Direction.Value : null;
                command.Parameters[7].Value = entity.BlockId;
                command.Parameters[8].Value = entity.ShapeId;
                command.Parameters[9].Value = entity.AccessibilityType.HasValue ? (int?)entity.AccessibilityType.Value : null;
                command.Parameters[10].Value = entityId;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                return command.ExecuteNonQuery() > 0;
            }
        }

        public void RemoveAll()
        {
            throw new NotImplementedException();
        }
    }
}