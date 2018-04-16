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
    /// Represents a collection of Stops using an SQLite database.
    /// </summary>
    public class PostgreSQLStopCollection : IUniqueEntityCollection<Stop>
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
        internal PostgreSQLStopCollection(NpgsqlConnection connection, int id)
        {
            _connection = connection;
            _id = id;
        }

        /// <summary>
        /// Adds an entity.
        /// </summary>
        /// <param name="entity"></param>
        public void Add(Stop entity)
        {
            string sql = "INSERT INTO stop VALUES (:feed_id, :id, :stop_code, :stop_name, :stop_desc, :stop_lat, :stop_lon, :zone_id, :stop_url, :location_type, :parent_station, :stop_timezone, :wheelchair_boarding);";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"stop_code", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"stop_name", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"stop_desc", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"stop_lat", DbType.Double));
                command.Parameters.Add(new NpgsqlParameter(@"stop_lon", DbType.Double));
                command.Parameters.Add(new NpgsqlParameter(@"zone_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"stop_url", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"location_type", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"parent_station", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"stop_timezone", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"wheelchair_boarding", DbType.String));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = entity.Id;
                command.Parameters[2].Value = entity.Code;
                command.Parameters[3].Value = entity.Name;
                command.Parameters[4].Value = entity.Description;
                command.Parameters[5].Value = entity.Latitude;
                command.Parameters[6].Value = entity.Longitude;
                command.Parameters[7].Value = entity.Zone;
                command.Parameters[8].Value = entity.Url;
                command.Parameters[9].Value = entity.LocationType.HasValue ? (int?)entity.LocationType.Value : null;
                command.Parameters[10].Value = entity.ParentStation;
                command.Parameters[11].Value = entity.Timezone;
                command.Parameters[12].Value = entity.WheelchairBoarding;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                command.ExecuteNonQuery();
            }
        }

        public void AddRange(IUniqueEntityCollection<Stop> entities)
        {
            using (var writer = _connection.BeginBinaryImport("COPY stop (feed_id, id, stop_code, stop_name, stop_desc, stop_lat, stop_lon, zone_id, stop_url, location_type, parent_station, stop_timezone, wheelchair_boarding) FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var stop in entities)
                {
                    writer.StartRow();
                    writer.Write(_id, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(stop.Id, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(stop.Code, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(stop.Name, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(stop.Description, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(stop.Latitude, NpgsqlTypes.NpgsqlDbType.Real);
                    writer.Write(stop.Longitude, NpgsqlTypes.NpgsqlDbType.Real);
                    writer.Write(stop.Zone, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(stop.Url, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(stop.LocationType, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(stop.ParentStation, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(stop.Timezone, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(stop.WheelchairBoarding, NpgsqlTypes.NpgsqlDbType.Text);
                }
            }
        }

        /// <summary>
        /// Gets the entity with the given id.
        /// </summary>
        /// <param name="entityId"></param>
        /// <returns></returns>
        public Stop Get(string entityId)
        {
            string sql = "SELECT id, stop_code, stop_name, stop_desc, stop_lat, stop_lon, zone_id, stop_url, location_type, parent_station, stop_timezone, wheelchair_boarding FROM stop WHERE FEED_ID = :id AND id = :entityId";
            var parameters = new List<NpgsqlParameter>();
            parameters.Add(new NpgsqlParameter(@"id", DbType.Int64));
            parameters[0].Value = _id;
            parameters.Add(new NpgsqlParameter(@"entityId", DbType.String));
            parameters[1].Value = entityId;

            return new PostgreSQLEnumerable<Stop>(_connection, sql, parameters.ToArray(), (x) =>
            {
                return new Stop()
                {
                    Id = x.GetString(0),
                    Code = x.IsDBNull(1) ? null : x.GetString(1),
                    Name = x.IsDBNull(2) ? null : x.GetString(2),
                    Description = x.IsDBNull(3) ? null : x.GetString(3),
                    Latitude = x.GetDouble(4),
                    Longitude = x.GetDouble(5),
                    Zone = x.IsDBNull(6) ? null : x.GetString(6),
                    Url = x.IsDBNull(7) ? null : x.GetString(7),
                    LocationType = x.IsDBNull(8) ? null : (LocationType?)x.GetInt64(8),
                    ParentStation = x.IsDBNull(9) ? null : x.GetString(9),
                    Timezone = x.IsDBNull(10) ? null : x.GetString(10),
                    WheelchairBoarding = x.IsDBNull(11) ? null : x.GetString(11)
                };
            }).FirstOrDefault();
        }

        /// <summary>
        /// Gets the entity at the given index.
        /// </summary>
        /// <param name="idx"></param>
        /// <returns></returns>
        public Stop Get(int idx)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Removes the entity with the given id.
        /// </summary>
        /// <param name="entityId"></param>
        /// <returns></returns>
        public bool Remove(string entityId)
        {
            string sql = "DELETE FROM stop WHERE FEED_ID = :feed_id AND id = :stop_id;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"stop_id", DbType.String));
                
                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = entityId;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                return command.ExecuteNonQuery() > 0;
            }
        }

        /// <summary>
        /// Removes a range of entities by their IDs
        /// </summary>
        /// <param name="entityId"></param>
        /// <returns></returns>
        public void RemoveRange(IEnumerable<string> entityIds)
        {
            using (var command = _connection.CreateCommand())
            {
                using (var transaction = _connection.BeginTransaction())
                {
                    foreach (var entityId in entityIds)
                    {
                        string sql = "DELETE FROM stop WHERE FEED_ID = :feed_id AND id = :stop_id;";
                        command.CommandText = sql;
                        command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                        command.Parameters.Add(new NpgsqlParameter(@"stop_id", DbType.String));

                        command.Parameters[0].Value = _id;
                        command.Parameters[1].Value = entityId;

                        command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                        command.ExecuteNonQuery();
                    }
                    transaction.Commit();
                }
            }
        }

        /// <summary>
        /// Removes all entities
        /// </summary>
        public void RemoveAll()
        {
            string sql = "DELETE FROM stop WHERE FEED_ID = :feed_id;";
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
        /// Edits the entity with the given id.
        /// </summary>
        /// <param name="entityId"></param>
        /// <returns></returns>
        public bool Update(string entityId, Stop entity)
        {
            string sql = "UPDATE stop SET FEED_ID=:feed_id, id=:id, stop_code=:stop_code, stop_name=:stop_name, stop_desc=:stop_desc, stop_lat=:stop_lat, stop_lon=:stop_lon, zone_id=:zone_id, stop_url=:stop_url, location_type=:location_type, parent_station=:parent_station, stop_timezone=:stop_timezone, wheelchair_boarding=:wheelchair_boarding WHERE id=:entityId;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"stop_code", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"stop_name", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"stop_desc", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"stop_lat", DbType.Double));
                command.Parameters.Add(new NpgsqlParameter(@"stop_lon", DbType.Double));
                command.Parameters.Add(new NpgsqlParameter(@"zone_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"stop_url", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"location_type", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"parent_station", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"stop_timezone", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"wheelchair_boarding", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"entityId", DbType.String));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = entity.Id;
                command.Parameters[2].Value = entity.Code;
                command.Parameters[3].Value = entity.Name;
                command.Parameters[4].Value = entity.Description;
                command.Parameters[5].Value = entity.Latitude;
                command.Parameters[6].Value = entity.Longitude;
                command.Parameters[7].Value = entity.Zone;
                command.Parameters[8].Value = entity.Url;
                command.Parameters[9].Value = entity.LocationType.HasValue ? (int?)entity.LocationType.Value : null;
                command.Parameters[10].Value = entity.ParentStation;
                command.Parameters[11].Value = entity.Timezone;
                command.Parameters[12].Value = entity.WheelchairBoarding;
                command.Parameters[13].Value = entityId;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                if (command.ExecuteNonQuery() <= 0) return false;
            }

            //update cleaned_stops if the stop_id changed
            if (!entityId.Equals(entity.Id))
            {
                sql = "UPDATE cleaned_stops SET stop_id=:stop_id WHERE stop_id=:entityId;";
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = sql;
                    command.Parameters.Add(new NpgsqlParameter(@"stop_id", DbType.String));
                    command.Parameters.Add(new NpgsqlParameter(@"entityId", DbType.String));

                    command.Parameters[0].Value = entity.Id;
                    command.Parameters[1].Value = entityId;

                    command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                    return command.ExecuteNonQuery() > 0;
                }
            }

            return true;
        }

        /// <summary>
        /// Returns all entities.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Stop> Get()
        {
            #if DEBUG
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            Console.Write($"Fetching stops...");
            #endif
            var stops = new List<Stop>();
            using (var reader = _connection.BeginBinaryExport("COPY stop TO STDOUT (FORMAT BINARY)"))
            {
                while (reader.StartRow() > 0)
                {
                    var feedId = reader.Read<int>(NpgsqlTypes.NpgsqlDbType.Integer);
                    stops.Add(new Stop()
                    {
                        Id = reader.ReadStringSafe(),
                        Code = reader.ReadStringSafe(),
                        Name = reader.ReadStringSafe(),
                        Description = reader.ReadStringSafe(),
                        Latitude = reader.Read<double>(NpgsqlTypes.NpgsqlDbType.Real),
                        Longitude = reader.Read<double>(NpgsqlTypes.NpgsqlDbType.Real),
                        Zone = reader.ReadStringSafe(),
                        Url = reader.ReadStringSafe(),
                        LocationType = (LocationType?)reader.ReadIntSafe(),
                        ParentStation = reader.ReadStringSafe(),
                        Timezone = reader.ReadStringSafe(),
                        WheelchairBoarding = reader.ReadStringSafe()
                    });
                }
            }
            #if DEBUG
            stopwatch.Stop();
            Console.WriteLine($" {stopwatch.ElapsedMilliseconds} ms");
            #endif
            return stops;
        }

        /// <summary>
        /// Returns the number of entities.
        /// </summary>
        public int Count
        {
            get { throw new NotImplementedException(); }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns></returns>
        public IEnumerator<Stop> GetEnumerator()
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