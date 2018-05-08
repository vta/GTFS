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
using System.Text;

namespace GTFS.DB.PostgreSQL.Collections
{
    /// <summary>
    /// Represents a collection of StopTimes using an SQLite database.
    /// </summary>
    public class PostgreSQLStopTimeCollection : IStopTimeCollection
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
        internal PostgreSQLStopTimeCollection(NpgsqlConnection connection, int id)
        {
            _connection = connection;
            _id = id;
        }

        /// <summary>
        /// Adds an entity.
        /// </summary>
        /// <param name="stopTime"></param>
        public void Add(StopTime stopTime)
        {
            string sql = "INSERT INTO stop_time VALUES (:feed_id, :trip_id, :arrival_time, :departure_time, :stop_id, :stop_sequence, :stop_headsign, :pickup_type, :drop_off_type, :shape_dist_traveled, :passenger_boarding, :passenger_alighting);";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"trip_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"arrival_time", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"departure_time", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"stop_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"stop_sequence", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"stop_headsign", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"pickup_type", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"drop_off_type", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"shape_dist_traveled", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"passenger_boarding", DbType.Int32));
                command.Parameters.Add(new NpgsqlParameter(@"passenger_alighting", DbType.Int32));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = stopTime.TripId;
                command.Parameters[2].Value = stopTime.ArrivalTime.TotalSeconds;
                command.Parameters[3].Value = stopTime.DepartureTime.TotalSeconds;
                command.Parameters[4].Value = stopTime.StopId;
                command.Parameters[5].Value = stopTime.StopSequence;
                command.Parameters[6].Value = stopTime.StopHeadsign;
                command.Parameters[7].Value = stopTime.PickupType.HasValue ? (int?)stopTime.PickupType.Value : null;
                command.Parameters[8].Value = stopTime.DropOffType.HasValue ? (int?)stopTime.DropOffType.Value : null;
                command.Parameters[9].Value = stopTime.ShapeDistTravelled;
                command.Parameters[10].Value = stopTime.PassengerBoarding;
                command.Parameters[11].Value = stopTime.PassengerAlighting;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                command.ExecuteNonQuery();
            }
        }

        public void AddRange(IEnumerable<StopTime> entities)
        {
            using (var writer = _connection.BeginBinaryImport("COPY stop_time (feed_id, trip_id, arrival_time, departure_time, stop_id, stop_sequence, stop_headsign, pickup_type, drop_off_type, shape_dist_traveled, passenger_boarding, passenger_alighting) FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var stopTime in entities)
                {
                    writer.StartRow();
                    writer.Write(_id, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(stopTime.TripId, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(stopTime.ArrivalTime.TotalSeconds, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(stopTime.DepartureTime.TotalSeconds, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(stopTime.StopId, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(stopTime.StopSequence, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(stopTime.StopHeadsign, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(stopTime.PickupType, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(stopTime.DropOffType, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(stopTime.ShapeDistTravelled, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(stopTime.PassengerBoarding, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(stopTime.PassengerAlighting, NpgsqlTypes.NpgsqlDbType.Integer);
                }
            }
        }

        public bool Update(string stopId, string tripId, StopTime newEntity)
        {
            string sql = "UPDATE stop_time SET FEED_ID=:feed_id, trip_id=:trip_id, arrival_time=:arrival_time, departure_time=:departure_time, stop_id=:stop_id, stop_sequence=:stop_sequence, stop_headsign=:stop_headsign, pickup_type=:pickup_type, drop_off_type=:drop_off_type, shape_dist_traveled=:shape_dist_traveled, passenger_boarding=:passenger_boarding, passenger_alighting=:passenger_alighting WHERE stop_id=:oldStopId AND trip_id=:oldTripId;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"trip_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"arrival_time", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"departure_time", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"stop_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"stop_sequence", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"stop_headsign", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"pickup_type", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"drop_off_type", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"shape_dist_traveled", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"passenger_boarding", DbType.Int32));
                command.Parameters.Add(new NpgsqlParameter(@"passenger_alighting", DbType.Int32));
                command.Parameters.Add(new NpgsqlParameter(@"oldStopId", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"oldTripId", DbType.String));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = newEntity.TripId;
                command.Parameters[2].Value = newEntity.ArrivalTime.TotalSeconds;
                command.Parameters[3].Value = newEntity.DepartureTime.TotalSeconds;
                command.Parameters[4].Value = newEntity.StopId;
                command.Parameters[5].Value = newEntity.StopSequence;
                command.Parameters[6].Value = newEntity.StopHeadsign;
                command.Parameters[7].Value = newEntity.PickupType;
                command.Parameters[8].Value = newEntity.DropOffType;
                command.Parameters[9].Value = newEntity.ShapeDistTravelled;
                command.Parameters[10].Value = newEntity.PassengerBoarding;
                command.Parameters[11].Value = newEntity.PassengerAlighting;
                command.Parameters[12].Value = stopId;
                command.Parameters[13].Value = tripId;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                return command.ExecuteNonQuery() > 0;
            }
        }

        public bool Update(string stopId, string tripId, uint stopSequence, StopTime newEntity)
        {
            string sql = "UPDATE stop_time SET FEED_ID=:feed_id, trip_id=:trip_id, arrival_time=:arrival_time, departure_time=:departure_time, stop_id=:stop_id, stop_sequence=:stop_sequence, stop_headsign=:stop_headsign, pickup_type=:pickup_type, drop_off_type=:drop_off_type, shape_dist_traveled=:shape_dist_traveled, passenger_boarding=:passenger_boarding, passenger_alighting=:passenger_alighting WHERE stop_id=:oldStopId AND trip_id=:oldTripId AND stop_sequence=:oldStopSequence;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"trip_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"arrival_time", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"departure_time", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"stop_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"stop_sequence", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"stop_headsign", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"pickup_type", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"drop_off_type", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"shape_dist_traveled", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"passenger_boarding", DbType.Int32));
                command.Parameters.Add(new NpgsqlParameter(@"passenger_alighting", DbType.Int32));
                command.Parameters.Add(new NpgsqlParameter(@"oldStopId", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"oldTripId", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"oldStopSequence", DbType.String));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = newEntity.TripId;
                command.Parameters[2].Value = newEntity.ArrivalTime.TotalSeconds;
                command.Parameters[3].Value = newEntity.DepartureTime.TotalSeconds;
                command.Parameters[4].Value = newEntity.StopId;
                command.Parameters[5].Value = newEntity.StopSequence;
                command.Parameters[6].Value = newEntity.StopHeadsign;
                command.Parameters[7].Value = newEntity.PickupType;
                command.Parameters[8].Value = newEntity.DropOffType;
                command.Parameters[9].Value = newEntity.ShapeDistTravelled;
                command.Parameters[10].Value = newEntity.PassengerBoarding;
                command.Parameters[11].Value = newEntity.PassengerAlighting;
                command.Parameters[12].Value = stopId;
                command.Parameters[13].Value = tripId;
                command.Parameters[14].Value = stopSequence;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                return command.ExecuteNonQuery() > 0;
            }
        }

        public void RemoveRange(IEnumerable<StopTime> entities)
        {
            using (var command = _connection.CreateCommand())
            {
                using (var transaction = _connection.BeginTransaction())
                {
                    foreach (var stopTime in entities)
                    {
                        string sql = "DELETE FROM stop_time WHERE FEED_ID = :feed_id AND trip_id = :trip_id AND arrival_time = :arrival_time AND departure_time = :departure_time and stop_id = :stop_id AND stop_sequence = :stop_sequence;";
                        command.CommandText = sql;
                        command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                        command.Parameters.Add(new NpgsqlParameter(@"trip_id", DbType.String));
                        command.Parameters.Add(new NpgsqlParameter(@"arrival_time", DbType.Int64));
                        command.Parameters.Add(new NpgsqlParameter(@"departure_time", DbType.Int64));
                        command.Parameters.Add(new NpgsqlParameter(@"stop_id", DbType.String));
                        command.Parameters.Add(new NpgsqlParameter(@"stop_sequence", DbType.Int64));
                        
                        command.Parameters[0].Value = _id;
                        command.Parameters[1].Value = stopTime.TripId;
                        command.Parameters[2].Value = stopTime.ArrivalTime.TotalSeconds;
                        command.Parameters[3].Value = stopTime.DepartureTime.TotalSeconds;
                        command.Parameters[4].Value = stopTime.StopId;
                        command.Parameters[5].Value = stopTime.StopSequence;

                        command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                        command.ExecuteNonQuery();
                    }
                    transaction.Commit();
                }
            }
        }

        public void RemoveAll()
        {
            string sql = "DELETE from stop_time WHERE FEED_ID = :feed_id;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));

                command.Parameters[0].Value = _id;
                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Returns all stop times.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<StopTime> Get()
        {
            #if DEBUG
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            #endif
            var stopTimes = new List<StopTime>();
            using (var reader = _connection.BeginBinaryExport("COPY stop_time TO STDOUT (FORMAT BINARY)"))
            {
                while (reader.StartRow() > 0)
                {
                    var feedId = reader.Read<int>(NpgsqlTypes.NpgsqlDbType.Integer);
                    stopTimes.Add(new StopTime()
                    {
                        TripId = reader.ReadStringSafe(),
                        ArrivalTime = TimeOfDay.FromTotalSeconds(reader.Read<int>(NpgsqlTypes.NpgsqlDbType.Integer)),
                        DepartureTime = TimeOfDay.FromTotalSeconds(reader.Read<int>(NpgsqlTypes.NpgsqlDbType.Integer)),
                        StopId = reader.ReadStringSafe(),
                        StopSequence = (uint)reader.Read<int>(NpgsqlTypes.NpgsqlDbType.Integer),
                        StopHeadsign = reader.ReadStringSafe(),
                        PickupType = (PickupType?)reader.ReadIntSafe(),
                        DropOffType = (DropOffType?)reader.ReadIntSafe(),
                        ShapeDistTravelled = reader.ReadStringSafe(),
                        PassengerBoarding = (int?)reader.ReadIntSafe(),
                        PassengerAlighting = (int?)reader.ReadIntSafe()
                    });
                }
            }
            #if DEBUG
            stopwatch.Stop();
            Console.WriteLine($"Fetch stoptimes: {stopwatch.ElapsedMilliseconds} ms");
            #endif
            return stopTimes;
        }

        /// <summary>
        /// Gets all stop times for the given trip.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<StopTime> GetForTrip(string tripId)
        {
            string sql = "SELECT trip_id, arrival_time, departure_time, stop_id, stop_sequence, stop_headsign, pickup_type, drop_off_type, shape_dist_traveled, passenger_boarding, passenger_alighting FROM stop_time WHERE FEED_ID = :id AND trip_id = :trip_id ORDER BY stop_sequence";
            var parameters = new List<NpgsqlParameter>();
            parameters.Add(new NpgsqlParameter(@"id", DbType.Int64));
            parameters[0].Value = _id;
            parameters.Add(new NpgsqlParameter(@"trip_id", DbType.String));
            parameters[1].Value = tripId;

            return new PostgreSQLEnumerable<StopTime>(_connection, sql, parameters.ToArray(), (x) =>
            {
                return new StopTime()
                {
                    TripId = x.GetString(0),
                    ArrivalTime = TimeOfDay.FromTotalSeconds(x.GetInt32(1)),
                    DepartureTime = TimeOfDay.FromTotalSeconds(x.GetInt32(2)),
                    StopId = x.GetString(3),
                    StopSequence = (uint)x.GetInt32(4),
                    StopHeadsign = x.IsDBNull(5) ? null : x.GetString(5),
                    PickupType = x.IsDBNull(6) ? null : (PickupType?)x.GetInt64(6),
                    DropOffType = x.IsDBNull(7) ? null : (DropOffType?)x.GetInt64(7),
                    ShapeDistTravelled = x.IsDBNull(8) ? null : x.GetString(8),
                    PassengerBoarding = x.IsDBNull(9) ? null : (int?)x.GetInt32(9),
                    PassengerAlighting = x.IsDBNull(10) ? null : (int?)x.GetInt32(10)
                };
            });
        }

        /// <summary>
        /// Gets all stop times for the given trips.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<StopTime> GetForTrips(IEnumerable<string> tripIds)
        {
            if (tripIds.Count() == 0)
            {
                return new List<StopTime>();
            }
            var sql = new StringBuilder("SELECT trip_id, arrival_time, departure_time, stop_id, stop_sequence, stop_headsign, pickup_type, drop_off_type, shape_dist_traveled, passenger_boarding, passenger_alighting FROM stop_time WHERE FEED_ID = :feed_id AND trip_id = :trip_id0");
            var parameters = new List<NpgsqlParameter>();
            parameters.Add(new NpgsqlParameter("feed_id", DbType.Int64));
            parameters[0].Value = _id;
            int i = 0;
            foreach (var tripId in tripIds)
            {
                if (i > 0) sql.Append($" OR trip_id = :trip_id{i}");
                parameters.Add(new NpgsqlParameter($"trip_id{i}", DbType.String));
                parameters[1 + i].Value = tripId;
                i++;
            }

            return new PostgreSQLEnumerable<StopTime>(_connection, sql.ToString(), parameters.ToArray(), (x) =>
            {
                return new StopTime()
                {
                    TripId = x.GetString(0),
                    ArrivalTime = TimeOfDay.FromTotalSeconds(x.GetInt32(1)),
                    DepartureTime = TimeOfDay.FromTotalSeconds(x.GetInt32(2)),
                    StopId = x.GetString(3),
                    StopSequence = (uint)x.GetInt32(4),
                    StopHeadsign = x.IsDBNull(5) ? null : x.GetString(5),
                    PickupType = x.IsDBNull(6) ? null : (PickupType?)x.GetInt64(6),
                    DropOffType = x.IsDBNull(7) ? null : (DropOffType?)x.GetInt64(7),
                    ShapeDistTravelled = x.IsDBNull(8) ? null : x.GetString(8),
                    PassengerBoarding = x.IsDBNull(9) ? null : (int?)x.GetInt32(9),
                    PassengerAlighting = x.IsDBNull(10) ? null : (int?)x.GetInt32(10)
                };
            });
        }

        /// <summary>
        /// Removes all stop times for the given trip.
        /// </summary>
        /// <returns></returns>
        public int RemoveForTrip(string tripId)
        {
            string sql = "DELETE FROM stop_time WHERE FEED_ID = :feed_id AND trip_id = :trip_id;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"trip_id", DbType.String));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = tripId;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                return command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Removes all stop times for the given set of trips.
        /// </summary>
        /// <returns></returns>
        public void RemoveForTrips(IEnumerable<string> tripIds)
        {
            using (var command = _connection.CreateCommand())
            {
                using (var transaction = _connection.BeginTransaction())
                {
                    foreach (var tripId in tripIds)
                    {
                        string sql = "DELETE FROM stop_time WHERE FEED_ID = :feed_id AND trip_id = :trip_id;";
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

        /// <summary>
        /// Gets all stop times for the given stop.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<StopTime> GetForStop(string stopId)
        {
            string sql = "SELECT trip_id, arrival_time, departure_time, stop_id, stop_sequence, stop_headsign, pickup_type, drop_off_type, shape_dist_traveled, passenger_boarding, passenger_alighting FROM stop_time WHERE FEED_ID = :id AND stop_id = :stop_id";
            var parameters = new List<NpgsqlParameter>();
            parameters.Add(new NpgsqlParameter(@"id", DbType.Int64));
            parameters[0].Value = _id;
            parameters.Add(new NpgsqlParameter(@"stop_id", DbType.String));
            parameters[1].Value = stopId;

            return new PostgreSQLEnumerable<StopTime>(_connection, sql, parameters.ToArray(), (x) =>
            {
                return new StopTime()
                {
                    TripId = x.GetString(0),
                    ArrivalTime = TimeOfDay.FromTotalSeconds(x.GetInt32(1)),
                    DepartureTime = TimeOfDay.FromTotalSeconds(x.GetInt32(2)),
                    StopId = x.GetString(3),
                    StopSequence = (uint)x.GetInt32(4),
                    StopHeadsign = x.IsDBNull(5) ? null : x.GetString(5),
                    PickupType = x.IsDBNull(6) ? null : (PickupType?)x.GetInt64(6),
                    DropOffType = x.IsDBNull(7) ? null : (DropOffType?)x.GetInt64(7),
                    ShapeDistTravelled = x.IsDBNull(8) ? null : x.GetString(8),
                    PassengerBoarding = x.IsDBNull(9) ? null : (int?)x.GetInt32(9),
                    PassengerAlighting = x.IsDBNull(10) ? null : (int?)x.GetInt32(10)
                };
            });
        }

        /// <summary>
        /// Removes all stop times for the given stop.
        /// </summary>
        /// <returns></returns>
        public int RemoveForStop(string stopId)
        {
            string sql = "DELETE FROM stop_time WHERE FEED_ID = :feed_id AND stop_id = :stop_id;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"stop_id", DbType.String));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = stopId;

                return command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns></returns>
        public IEnumerator<StopTime> GetEnumerator()
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
    }
}