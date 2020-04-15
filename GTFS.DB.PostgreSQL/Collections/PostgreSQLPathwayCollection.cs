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
    /// Represents a collection of Pathways using a Postgres database.
    /// </summary>
    public class PostgreSQLPathwayCollection : IUniqueEntityCollection<Pathway>
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
        internal PostgreSQLPathwayCollection(NpgsqlConnection connection, int id)
        {
            _connection = connection;
            _id = id;
        }

        /// <summary>
        /// Adds an entity.
        /// </summary>
        /// <param name="entity"></param>
        public void Add(Pathway entity)
        {
            string sql = "INSERT INTO pathway VALUES (:feed_id, :pathway_id, :from_stop_id, :to_stop_id, :pathway_mode, :is_bidirectional, :length, :traversal_time, :stair_count, :max_slope, :min_width, :signposted_as, :reversed_signposted_as );";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"pathway_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"from_stop_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"to_stop_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"pathway_mode", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"is_bidirectional", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"length", DbType.Double));
                command.Parameters.Add(new NpgsqlParameter(@"traversal_time", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"stair_count", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"max_slope", DbType.Double));
                command.Parameters.Add(new NpgsqlParameter(@"min_width", DbType.Double));
                command.Parameters.Add(new NpgsqlParameter(@"signposted_as", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"reversed_signposted_as", DbType.String));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = entity.Id;
                command.Parameters[2].Value = entity.FromStopId;
                command.Parameters[3].Value = entity.ToStopId;
                command.Parameters[4].Value = entity.PathwayMode;
                command.Parameters[5].Value = entity.IsBidirectional;
                command.Parameters[6].Value = entity.Length;
                command.Parameters[7].Value = entity.TraversalTime;
                command.Parameters[8].Value = entity.StairCount;
                command.Parameters[9].Value = entity.MaxSlope;
                command.Parameters[10].Value = entity.MinWidth;
                command.Parameters[11].Value = entity.SignpostedAs;
                command.Parameters[12].Value = entity.ReversedSignpostedAs;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                command.ExecuteNonQuery();
            }
        }


        public void AddRange(IUniqueEntityCollection<Pathway> entities)
        {
            using (var writer = _connection.BeginBinaryImport("COPY pathway (feed_id, pathway_id, from_stop_id, to_stop_id, pathway_mode, is_bidirectional, length, traversal_time, stair_count, max_slope, min_width, signposted_as, reversed_signposted_as) FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var pathway in entities)
                {
                    writer.StartRow();
                    writer.Write(_id, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(pathway.Id, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(pathway.FromStopId, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(pathway.ToStopId, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(pathway.PathwayMode, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(pathway.IsBidirectional, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(pathway.Length, NpgsqlTypes.NpgsqlDbType.Real);
                    writer.Write(pathway.TraversalTime, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(pathway.StairCount, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(pathway.MaxSlope, NpgsqlTypes.NpgsqlDbType.Real);
                    writer.Write(pathway.MinWidth, NpgsqlTypes.NpgsqlDbType.Real);
                    writer.Write(pathway.SignpostedAs, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(pathway.ReversedSignpostedAs, NpgsqlTypes.NpgsqlDbType.Text);
                }
            }
        }

        /// <summary>
        /// Gets the entity with the given id.
        /// </summary>
        /// <param name="entityId"></param>
        /// <returns></returns>
        public Pathway Get(string entityId)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the entity at the given index.
        /// </summary>
        /// <param name="idx"></param>
        /// <returns></returns>
        public Pathway Get(int idx)
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
            string sql = "DELETE FROM pathway WHERE FEED_ID = :feed_id AND pathway_id = :pathway_id;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"pathway_id", DbType.String));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = entityId;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                return command.ExecuteNonQuery() > 0;
            }
        }

        /// <summary>
        /// Returns all entities.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Pathway> Get()
        {
            string sql = "SELECT pathway_id, from_stop_id, to_stop_id, pathway_mode, is_bidirectional, length, traversal_time, stair_count, max_slope, min_width, signposted_as, reversed_signposted_as FROM pathway WHERE FEED_ID = :id";
            var parameters = new List<NpgsqlParameter>();
            parameters.Add(new NpgsqlParameter(@"id", DbType.Int64));
            parameters[0].Value = _id;

            return new PostgreSQLEnumerable<Pathway>(_connection, sql, parameters.ToArray(), (x) =>
            {
                return new Pathway()
                {
                    Id = x.GetString(0),
                    FromStopId = x.IsDBNull(1) ? null : x.GetString(1),
                    ToStopId = x.IsDBNull(2) ? null : x.GetString(2),
                    PathwayMode = (PathwayMode)x.GetInt16(3),
                    IsBidirectional = (IsBidirectional)x.GetInt16(4),
                    Length = x.IsDBNull(5) ? null : (double?)x.GetDouble(5),
                    TraversalTime = x.IsDBNull(6) ? null : (int?)x.GetInt32(6),
                    StairCount = x.IsDBNull(7) ? null : (int?)x.GetInt32(7),
                    MaxSlope = x.IsDBNull(8) ? null : (double?)x.GetDouble(8),
                    MinWidth = x.IsDBNull(9) ? null : (double?)x.GetDouble(9),
                    SignpostedAs = x.IsDBNull(10) ? null : x.GetString(10),
                    ReversedSignpostedAs = x.IsDBNull(11) ? null : x.GetString(11)
                };
            });
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
            Console.Write($"Fetching pathway ids...");
            #endif
            var outList = new List<string>();
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = "SELECT pathway_id FROM pathway";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        outList.Add(Convert.ToString(reader["pathway_id"]));
                    }
                }
            }
            #if DEBUG
            stopwatch.Stop();
            Console.WriteLine($" {stopwatch.ElapsedMilliseconds} ms");
            #endif
            return outList;
        }

        /// <summary>
        /// Returns the number of entities.
        /// </summary>
        public int Count
        {
            get
            {
                string sql = "SELECT count(pathway_id) FROM pathway;";
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = sql;
                    return int.Parse(command.ExecuteScalar().ToString());
                }
            }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns></returns>
        public IEnumerator<Pathway> GetEnumerator()
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

        public bool Update(string entityId, Pathway entity)
        {
            string sql = "UPDATE pathway SET FEED_ID=:feed_id, pathway_id=:pathway_id, from_stop_id=:from_stop_id, to_stop_id=:to_stop_id, pathway_mode=:pathway_mode, is_bidirectional=:is_bidirectional, length=:length, traversal_time=:traversal_time, stair_count=:stair_count, max_slope=:max_slope, min_width=:min_width, signposted_as=:signposted_as, reversed_signposted_as=:reversed_signposted_as WHERE pathway_id=:entityId;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"pathway_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"from_stop_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"to_stop_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"pathway_mode", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"is_bidirectional", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"length", DbType.Double));
                command.Parameters.Add(new NpgsqlParameter(@"traversal_time", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"stair_count", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"max_slope", DbType.Double));
                command.Parameters.Add(new NpgsqlParameter(@"min_width", DbType.Double));
                command.Parameters.Add(new NpgsqlParameter(@"signposted_as", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"reversed_signposted_as", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"entityId", DbType.String));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = entity.Id;
                command.Parameters[2].Value = entity.FromStopId;
                command.Parameters[3].Value = entity.ToStopId;
                command.Parameters[4].Value = entity.PathwayMode;
                command.Parameters[5].Value = entity.IsBidirectional;
                command.Parameters[6].Value = entity.Length;
                command.Parameters[7].Value = entity.TraversalTime;
                command.Parameters[8].Value = entity.StairCount;
                command.Parameters[9].Value = entity.MaxSlope;
                command.Parameters[10].Value = entity.MinWidth;
                command.Parameters[11].Value = entity.SignpostedAs;
                command.Parameters[12].Value = entity.ReversedSignpostedAs;
                command.Parameters[13].Value = entityId;

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
                    foreach (var entityId in entityIds)
                    {
                        string sql = "DELETE FROM pathway WHERE FEED_ID = :feed_id AND pathway_id = :pathway_id;";
                        command.CommandText = sql;
                        command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                        command.Parameters.Add(new NpgsqlParameter(@"pathway_id", DbType.String));

                        command.Parameters[0].Value = _id;
                        command.Parameters[1].Value = entityId;

                        command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                        command.ExecuteNonQuery();
                    }
                    transaction.Commit();
                }
            }
        }

        public void RemoveAll()
        {
            string sql = "DELETE FROM pathway WHERE FEED_ID = :feed_id;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));

                command.Parameters[0].Value = _id;
                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                command.ExecuteNonQuery();
            }
        }
    }
}