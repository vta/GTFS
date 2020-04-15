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
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace GTFS.DB.PostgreSQL.Collections
{
    /// <summary>
    /// Represents a collection of Shapes using a Postgres database.
    /// </summary>
    public class PostgreSQLShapeCollection : IEntityCollection<Shape>
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
        internal PostgreSQLShapeCollection(NpgsqlConnection connection, int id)
        {
            _connection = connection;
            _id = id;
        }

        /// <summary>
        /// Adds an entity.
        /// </summary>
        /// <param name="entity"></param>
        public void Add(Shape entity)
        {
            string sql = "INSERT INTO shape VALUES (:feed_id, :id, :shape_pt_lat, :shape_pt_lon, :shape_pt_sequence, :shape_dist_traveled);";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"shape_pt_lat", DbType.Double));
                command.Parameters.Add(new NpgsqlParameter(@"shape_pt_lon", DbType.Double));
                command.Parameters.Add(new NpgsqlParameter(@"shape_pt_sequence", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"shape_dist_traveled", DbType.Double));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = entity.Id;
                command.Parameters[2].Value = entity.Latitude;
                command.Parameters[3].Value = entity.Longitude;
                command.Parameters[4].Value = entity.Sequence;
                command.Parameters[5].Value = entity.DistanceTravelled;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Adds range of entities
        /// </summary>
        /// <param name="entities"></param>
        public void AddRange(IEntityCollection<Shape> entities)
        {
            using (var writer = _connection.BeginBinaryImport("COPY shape (feed_id, id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled) FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var shapePoint in entities)
                {
                    writer.StartRow();
                    writer.Write(_id, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(shapePoint.Id, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(shapePoint.Latitude, NpgsqlTypes.NpgsqlDbType.Real);
                    writer.Write(shapePoint.Longitude, NpgsqlTypes.NpgsqlDbType.Real);
                    writer.Write(shapePoint.Sequence, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(shapePoint.DistanceTravelled, NpgsqlTypes.NpgsqlDbType.Real);
                }
            }
        }

        /// <summary>
        /// Returns all entities.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Shape> Get()
        {
            #if DEBUG
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            Console.Write($"Fetching shapes...");
            #endif
            var shapePoints = new List<Shape>();
            using (var reader = _connection.BeginBinaryExport("COPY shape TO STDOUT (FORMAT BINARY)"))
            {
                while (reader.StartRow() > 0)
                {
                    var feedId = reader.Read<int>(NpgsqlTypes.NpgsqlDbType.Integer);
                    shapePoints.Add(new Shape()
                    {
                        Id = reader.ReadStringSafe(),
                        Latitude = reader.Read<double>(NpgsqlTypes.NpgsqlDbType.Real),
                        Longitude = reader.Read<double>(NpgsqlTypes.NpgsqlDbType.Real),
                        Sequence = (uint)reader.Read<int>(NpgsqlTypes.NpgsqlDbType.Integer),
                        DistanceTravelled = reader.ReadDoubleSafe()
                    });
                }
            }
            shapePoints = shapePoints.OrderBy(x => x.Id).ThenBy(x => x.Sequence).ToList();
            #if DEBUG
            stopwatch.Stop();
            Console.WriteLine($" {stopwatch.ElapsedMilliseconds} ms");
            #endif
            return shapePoints;
        }

        /// <summary>
        /// Returns all entities for the given id.
        /// </summary>
        /// <param name="entityId"></param>
        /// <returns></returns>
        public IEnumerable<Shape> Get(string entityId)
        {
            string sql = "SELECT id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled FROM shape WHERE FEED_ID = :id AND id = :shapeId ORDER BY shape_pt_sequence";
            var parameters = new List<NpgsqlParameter>();
            parameters.Add(new NpgsqlParameter(@"id", DbType.Int64));
            parameters[0].Value = _id;
            parameters.Add(new NpgsqlParameter(@"shapeId", DbType.String));
            parameters[1].Value = entityId;

            return new PostgreSQLEnumerable<Shape>(_connection, sql, parameters.ToArray(), (x) =>
            {
                return new Shape()
                {
                    Id = x.GetString(0),
                    Latitude = x.GetDouble(1),
                    Longitude = x.GetDouble(2),
                    Sequence = (uint)x.GetInt64(3),
                    DistanceTravelled = x.IsDBNull(4) ? null : (double?)x.GetDouble(4)
                };
            });
        }

        /// <summary>
        /// Returns the entities for the given id's.
        /// </summary>
        /// <param name="entityIds"></param>
        /// <returns></returns>
        public IEnumerable<Shape> Get(List<string> entityIds)
        {
            if (entityIds.Count() == 0)
            {
                return new List<Shape>();
            }

            var results = new List<Shape>();

            var groups = entityIds.SplitIntoGroupsByGroupIdx();
            foreach (var group in groups)
            {
                var sql = new StringBuilder("SELECT id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled FROM shape WHERE FEED_ID = :feed_id AND id = :id0");
                var parameters = new List<NpgsqlParameter>();
                parameters.Add(new NpgsqlParameter("feed_id", DbType.Int64));
                parameters[0].Value = _id;
                int i = 0;
                foreach (var entityId in group.Value)
                {
                    if (i > 0) sql.Append($" OR id = :id{i}");
                    parameters.Add(new NpgsqlParameter($"id{i}", DbType.String));
                    parameters[1 + i].Value = entityId;
                    i++;
                }
                sql.Append(" ORDER BY id, shape_pt_sequence");

                var result = new PostgreSQLEnumerable<Shape>(_connection, sql.ToString(), parameters.ToArray(), (x) =>
                {
                    return new Shape()
                    {
                        Id = x.GetString(0),
                        Latitude = x.GetDouble(1),
                        Longitude = x.GetDouble(2),
                        Sequence = (uint)x.GetInt64(3),
                        DistanceTravelled = x.IsDBNull(4) ? null : (double?)x.GetDouble(4)
                    };
                });
                results.AddRange(result);
            }

            return results;
        }

        /// <summary>
        /// Removes all entities identified by the given id.
        /// </summary>
        /// <param name="entityId"></param>
        /// <returns></returns>
        public bool Remove(string entityId)
        {
            string sql = "DELETE FROM shape WHERE FEED_ID = :feed_id AND id = :shape_id;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"shape_id", DbType.String));


                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = entityId;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                return command.ExecuteNonQuery() > 0;
            }
        }

        /// <summary>
        /// Removes a range of entities by their IDs
        /// </summary>
        /// <param name="entityIds"></param>
        /// <returns></returns>
        public void RemoveRange(IEnumerable<string> entityIds)
        {
            using (var command = _connection.CreateCommand())
            {
                using (var transaction = _connection.BeginTransaction())
                {
                    foreach (var entityId in entityIds)
                    {
                        string sql = "DELETE FROM shape WHERE FEED_ID = :feed_id AND id = :shape_id;";
                        command.CommandText = sql;
                        command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                        command.Parameters.Add(new NpgsqlParameter(@"shape_id", DbType.String));

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
        /// <returns></returns>
        public void RemoveAll()
        {
            string sql = "DELETE from shape WHERE FEED_ID = :feed_id;";
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
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns></returns>
        public IEnumerator<Shape> GetEnumerator()
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

        /// <summary>
        /// Returns entity ids
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetIds()
        {
            #if DEBUG
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            Console.Write($"Fetching shape ids...");
            #endif
            var shapeIds = new List<string>();
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = "SELECT DISTINCT(id) FROM shape";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        shapeIds.Add(Convert.ToString(reader["id"]));
                    }
                }
            }
            #if DEBUG
            stopwatch.Stop();
            Console.WriteLine($" {stopwatch.ElapsedMilliseconds} ms");
            #endif
            return shapeIds;
        }
    }
}