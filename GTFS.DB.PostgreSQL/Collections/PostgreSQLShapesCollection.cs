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

using Dapper;
using GTFS.Entities;
using GTFS.Entities.Collections;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace GTFS.DB.PostgreSQL.Collections
{
    /// <summary>
    /// Represents a collection of Shapes using an SQLite database.
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
        /// Caches the shapes in memory
        /// </summary>
        private static List<Shape> CachedShapes { get; set; }
        private static int CacheVersion { get; set; } = -1;
        private int GetCurrentCacheVersion()
        {
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = "SELECT shape_version FROM cache_versions;";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var str = Convert.ToString(reader["shape_version"]);
                        int.TryParse(str, out int currentVersion);
                        return currentVersion;
                    }
                }
            }
            return 0;
        }

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
            /*using (var writer = _connection.BeginBinaryImport("COPY shape (feed_id, id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled) FROM STDIN (FORMAT BINARY)"))
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
            }*/

            var shapeGroups = entities.GroupBy(x => x.Id);
            foreach (var group in shapeGroups)
            {
                var shapePoints = group.ToList();
                var shape = new PostgisLineString(group.Select(x => new Coordinate2D(x.Longitude, x.Latitude)));
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = @"INSERT INTO shape_gis VALUES (:feed_id, :id, :shape)";
                    command.Parameters.AddWithValue(@"feed_id", NpgsqlDbType.Integer, _id);
                    command.Parameters.AddWithValue(@"id", NpgsqlDbType.Text, group.Key);
                    command.Parameters.AddWithValue(@"shape", NpgsqlDbType.Geometry, shape);
                    command.ExecuteNonQuery();
                }
            }
        }


        internal class GeometryDbModel
        {
            public string id { get; set; }
            public PostgisLineString shape { get; set; }
        }

        /// <summary>
        /// Returns all entities.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Shape> Get()
        {
            var queryString = @"SELECT id, shape FROM shape_gis";
            var result = _connection.Query<GeometryDbModel>(queryString);
            var shpPoints = new List<Shape>();
            foreach (var shape in result)
            {
                uint i = 1;
                shpPoints.AddRange(shape.shape.Select(x => new Shape()
                {
                    Id = shape.id,
                    Latitude = x.Y,
                    Longitude = x.X,
                    Sequence = i++
                }));
            }
            return shpPoints;
            
            #if DEBUG
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            Console.Write($"Fetching shapes...");
            #endif
            var shapePoints = new List<Shape>();
            var currentVersion = GetCurrentCacheVersion();
            if (currentVersion == CacheVersion)
            {
                #if DEBUG
                Console.Write($" Found cached shapes...");
                #endif
                shapePoints = CachedShapes;
            }
            else
            {
                using (var reader = _connection.BeginBinaryExport("COPY shape TO STDOUT (FORMAT BINARY)"))
                {
                    while (reader.StartRow() > 0)
                    {
                        var feedId = reader.Read<int>(NpgsqlTypes.NpgsqlDbType.Integer);
                        shapePoints.Add(new Shape()
                        {
                            Id = reader.Read<string>(NpgsqlTypes.NpgsqlDbType.Text),
                            Latitude = reader.Read<double>(NpgsqlTypes.NpgsqlDbType.Real),
                            Longitude = reader.Read<double>(NpgsqlTypes.NpgsqlDbType.Real),
                            Sequence = (uint)reader.Read<int>(NpgsqlTypes.NpgsqlDbType.Integer),
                            DistanceTravelled = reader.ReadDoubleSafe()
                        });
                    }
                }
                CachedShapes = shapePoints;
                CacheVersion = currentVersion;
            }
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
            string sql = "SELECT id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled FROM shape WHERE FEED_ID = :id AND id = :shapeId";
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
    }
}