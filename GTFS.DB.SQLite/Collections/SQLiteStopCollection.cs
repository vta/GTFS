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

using GTFS.Entities;
using GTFS.Entities.Collections;
using GTFS.Entities.Enumerations;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Linq;

namespace GTFS.DB.SQLite.Collections
{
    /// <summary>
    /// Represents a collection of Stops using an SQLite database.
    /// </summary>
    public class SQLiteStopCollection : IUniqueEntityCollection<Stop>
    {
        /// <summary>
        /// Holds the connection.
        /// </summary>
        private SQLiteConnection _connection;

        /// <summary>
        /// Holds the id.
        /// </summary>
        private int _id;

        /// <summary>
        /// Creates a new SQLite GTFS feed.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="id"></param>
        internal SQLiteStopCollection(SQLiteConnection connection, int id)
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
            string sql = "INSERT INTO stop VALUES (:feed_id, :id, :stop_code, :stop_name, :stop_desc, :stop_lat, :stop_lon, :zone_id, :stop_url, :location_type, :parent_station, :stop_timezone, :wheelchair_boarding, :level_id, :platform_code);";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new SQLiteParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new SQLiteParameter(@"id", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"stop_code", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"stop_name", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"stop_desc", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"stop_lat", DbType.Double));
                command.Parameters.Add(new SQLiteParameter(@"stop_lon", DbType.Double));
                command.Parameters.Add(new SQLiteParameter(@"zone_id", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"stop_url", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"location_type", DbType.Int64));
                command.Parameters.Add(new SQLiteParameter(@"parent_station", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"stop_timezone", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"wheelchair_boarding", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"level_id", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"platform_code", DbType.String));

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
                command.Parameters[13].Value = entity.LevelId;
                command.Parameters[14].Value = entity.PlatformCode;

                command.ExecuteNonQuery();
            }
        }

        public void AddRange(IUniqueEntityCollection<Stop> entities)
        {
            using (var command = _connection.CreateCommand())
            {
                using (var transaction = _connection.BeginTransaction())
                {
                    foreach (var entity in entities)
                    {
                        string sql = "INSERT INTO stop VALUES (:feed_id, :id, :stop_code, :stop_name, :stop_desc, :stop_lat, :stop_lon, :zone_id, :stop_url, :location_type, :parent_station, :stop_timezone, :wheelchair_boarding, :level_id, :platform_code);";
                        command.CommandText = sql;
                        command.Parameters.Add(new SQLiteParameter(@"feed_id", DbType.Int64));
                        command.Parameters.Add(new SQLiteParameter(@"id", DbType.String));
                        command.Parameters.Add(new SQLiteParameter(@"stop_code", DbType.String));
                        command.Parameters.Add(new SQLiteParameter(@"stop_name", DbType.String));
                        command.Parameters.Add(new SQLiteParameter(@"stop_desc", DbType.String));
                        command.Parameters.Add(new SQLiteParameter(@"stop_lat", DbType.Double));
                        command.Parameters.Add(new SQLiteParameter(@"stop_lon", DbType.Double));
                        command.Parameters.Add(new SQLiteParameter(@"zone_id", DbType.String));
                        command.Parameters.Add(new SQLiteParameter(@"stop_url", DbType.String));
                        command.Parameters.Add(new SQLiteParameter(@"location_type", DbType.Int64));
                        command.Parameters.Add(new SQLiteParameter(@"parent_station", DbType.String));
                        command.Parameters.Add(new SQLiteParameter(@"stop_timezone", DbType.String));
                        command.Parameters.Add(new SQLiteParameter(@"wheelchair_boarding", DbType.String));
                        command.Parameters.Add(new SQLiteParameter(@"level_id", DbType.String));
                        command.Parameters.Add(new SQLiteParameter(@"platform_code", DbType.String));

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
                        command.Parameters[13].Value = entity.LevelId;
                        command.Parameters[14].Value = entity.PlatformCode;

                        command.ExecuteNonQuery();
                    }
                    transaction.Commit();
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
            string sql = "SELECT id, stop_code, stop_name, stop_desc, stop_lat, stop_lon, zone_id, stop_url, location_type, parent_station, stop_timezone, wheelchair_boarding, level_id, platform_code FROM stop WHERE FEED_ID = :id AND id = :entityId";
            var parameters = new List<SQLiteParameter>();
            parameters.Add(new SQLiteParameter(@"id", DbType.Int64));
            parameters[0].Value = _id;
            parameters.Add(new SQLiteParameter(@"entityId", DbType.String));
            parameters[1].Value = entityId;

            return new SQLiteEnumerable<Stop>(_connection, sql, parameters.ToArray(), (x) =>
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
                    WheelchairBoarding = x.IsDBNull(11) ? null : x.GetString(11),
                    LevelId = x.IsDBNull(12) ? null : x.GetString(12),
                    PlatformCode = x.IsDBNull(13) ? null : x.GetString(13),
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
                command.Parameters.Add(new SQLiteParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new SQLiteParameter(@"stop_id", DbType.String));
                
                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = entityId;

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
                        command.Parameters.Add(new SQLiteParameter(@"feed_id", DbType.Int64));
                        command.Parameters.Add(new SQLiteParameter(@"stop_id", DbType.String));

                        command.Parameters[0].Value = _id;
                        command.Parameters[1].Value = entityId;
                        
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
                command.Parameters.Add(new SQLiteParameter(@"feed_id", DbType.Int64));                

                command.Parameters[0].Value = _id;
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
            string sql = "UPDATE stop SET FEED_ID=:feed_id, id=:id, stop_code=:stop_code, stop_name=:stop_name, stop_desc=:stop_desc, stop_lat=:stop_lat, stop_lon=:stop_lon, zone_id=:zone_id, stop_url=:stop_url, location_type=:location_type, parent_station=:parent_station, stop_timezone=:stop_timezone, wheelchair_boarding=:wheelchair_boarding, level_id=:level_id, platform_code=:platform_code WHERE id=:entityId;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new SQLiteParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new SQLiteParameter(@"id", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"stop_code", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"stop_name", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"stop_desc", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"stop_lat", DbType.Double));
                command.Parameters.Add(new SQLiteParameter(@"stop_lon", DbType.Double));
                command.Parameters.Add(new SQLiteParameter(@"zone_id", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"stop_url", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"location_type", DbType.Int64));
                command.Parameters.Add(new SQLiteParameter(@"parent_station", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"stop_timezone", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"wheelchair_boarding", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"level_id", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"platform_code", DbType.String));
                command.Parameters.Add(new SQLiteParameter(@"entityId", DbType.String));

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
                command.Parameters[13].Value = entity.LevelId;
                command.Parameters[14].Value = entity.PlatformCode;
                command.Parameters[15].Value = entityId;

                if (command.ExecuteNonQuery() <= 0)
                {
                    return false;
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
            string sql = "SELECT id, stop_code, stop_name, stop_desc, stop_lat, stop_lon, zone_id, stop_url, location_type, parent_station, stop_timezone, wheelchair_boarding, level_id, platform_code FROM stop WHERE FEED_ID = :id";
            var parameters = new List<SQLiteParameter>();
            parameters.Add(new SQLiteParameter(@"id", DbType.Int64));
            parameters[0].Value = _id;

            return new SQLiteEnumerable<Stop>(_connection, sql, parameters.ToArray(), (x) =>
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
                    WheelchairBoarding = x.IsDBNull(11) ? null : x.GetString(11),
                    LevelId = x.IsDBNull(12) ? null : x.GetString(12),
                    PlatformCode = x.IsDBNull(13) ? null : x.GetString(13)
                };
            });
        }

        /// <summary>
        /// Returns entity ids
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetIds()
        {
            var outList = new List<string>();
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = "SELECT id FROM stop";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        outList.Add(Convert.ToString(reader["id"]));
                    }
                }
            }
            return outList;
        }

        public IEnumerable<Stop> GetChildStops(string parentStationId)
        {
            string sql = "SELECT id, stop_code, stop_name, stop_desc, stop_lat, stop_lon, zone_id, stop_url, location_type, parent_station, stop_timezone, wheelchair_boarding, level_id, platform_code FROM stop WHERE FEED_ID = :id AND parent_station = :parent_station";
            var parameters = new List<SQLiteParameter>();
            parameters.Add(new SQLiteParameter(@"id", DbType.Int64));
            parameters[0].Value = _id;
            parameters.Add(new SQLiteParameter(@"parent_station", DbType.String));
            parameters[1].Value = parentStationId;

            return new SQLiteEnumerable<Stop>(_connection, sql, parameters.ToArray(), (x) =>
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
                    WheelchairBoarding = x.IsDBNull(11) ? null : x.GetString(11),
                    LevelId = x.IsDBNull(12) ? null : x.GetString(12),
                    PlatformCode = x.IsDBNull(13) ? null : x.GetString(13)
                };
            });
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