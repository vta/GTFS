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
    /// Represents a collection of Levels using a Postgres database.
    /// </summary>
    public class PostgreSQLLevelCollection : IUniqueEntityCollection<Level>
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
        internal PostgreSQLLevelCollection(NpgsqlConnection connection, int id)
        {
            _connection = connection;
            _id = id;
        }

        /// <summary>
        /// Adds an entity.
        /// </summary>
        /// <param name="entity"></param>
        public void Add(Level entity)
        {
            string sql = "INSERT INTO level VALUES (:feed_id, :level_id, :level_index, :level_name);";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"level_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"level_index", DbType.Double));
                command.Parameters.Add(new NpgsqlParameter(@"level_name", DbType.String));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = entity.Id;
                command.Parameters[2].Value = entity.Index;
                command.Parameters[3].Value = entity.Name;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                command.ExecuteNonQuery();
            }
        }


        public void AddRange(IUniqueEntityCollection<Level> entities)
        {
            using (var writer = _connection.BeginBinaryImport("COPY level (feed_id, level_id, level_index, level_name) FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var level in entities)
                {
                    writer.StartRow();
                    writer.Write(_id, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(level.Id, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(level.Index, NpgsqlTypes.NpgsqlDbType.Real);
                    writer.Write(level.Name, NpgsqlTypes.NpgsqlDbType.Text);
                }
            }
        }

        /// <summary>
        /// Gets the entity with the given id.
        /// </summary>
        /// <param name="entityId"></param>
        /// <returns></returns>
        public Level Get(string entityId)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the entity at the given index.
        /// </summary>
        /// <param name="idx"></param>
        /// <returns></returns>
        public Level Get(int idx)
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
            string sql = "DELETE FROM level WHERE FEED_ID = :feed_id AND level_id = :level_id;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"level_id", DbType.String));

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
        public IEnumerable<Level> Get()
        {
            string sql = "SELECT level_id, level_index, level_name FROM level WHERE FEED_ID = :id";
            var parameters = new List<NpgsqlParameter>();
            parameters.Add(new NpgsqlParameter(@"id", DbType.Int64));
            parameters[0].Value = _id;

            return new PostgreSQLEnumerable<Level>(_connection, sql, parameters.ToArray(), (x) =>
            {
                return new Level()
                {
                    Id = x.GetString(0),
                    Index = x.GetDouble(1),
                    Name = x.IsDBNull(2) ? null : x.GetString(2)
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
            Console.Write($"Fetching level ids...");
            #endif
            var outList = new List<string>();
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = "SELECT level_id FROM level";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        outList.Add(Convert.ToString(reader["level_id"]));
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
                string sql = "SELECT count(level_id) FROM level;";
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
        public IEnumerator<Level> GetEnumerator()
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

        public bool Update(string entityId, Level entity)
        {
            string sql = "UPDATE level SET FEED_ID=:feed_id, level_id=:level_id, level_index=:level_index, level_name=:level_name WHERE level_id=:entityId;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"level_index", DbType.Double));
                command.Parameters.Add(new NpgsqlParameter(@"level_name", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"entityId", DbType.String));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = entity.Id;
                command.Parameters[2].Value = entity.Index;
                command.Parameters[3].Value = entity.Name;
                command.Parameters[4].Value = entityId;

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
                        string sql = "DELETE FROM level WHERE FEED_ID = :feed_id AND level_id = :level_id;";
                        command.CommandText = sql;
                        command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                        command.Parameters.Add(new NpgsqlParameter(@"level_id", DbType.String));

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
            throw new NotImplementedException();
        }
    }
}