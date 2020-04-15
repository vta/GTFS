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
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace GTFS.DB.PostgreSQL.Collections
{
    /// <summary>
    /// Represents a collection of FareRules using a Postgres database.
    /// </summary>
    public class PostgreSQLFareRuleCollection : IUniqueEntityCollection<FareRule>
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
        internal PostgreSQLFareRuleCollection(NpgsqlConnection connection, int id)
        {
            _connection = connection;
            _id = id;
        }

        /// <summary>
        /// Adds an entity.
        /// </summary>
        /// <param name="entity"></param>
        public void Add(FareRule entity)
        {
            string sql = "INSERT INTO fare_rule VALUES (:feed_id, :fare_id, :route_id, :origin_id, :destination_id, :contains_id);";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"fare_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"route_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"origin_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"destination_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"contains_id", DbType.String));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = entity.FareId;
                command.Parameters[2].Value = entity.RouteId;
                command.Parameters[3].Value = entity.OriginId;
                command.Parameters[4].Value = entity.DestinationId;
                command.Parameters[5].Value = entity.ContainsId;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Gets the entity with the given id.
        /// </summary>
        /// <param name="entityId"></param>
        /// <returns></returns>
        public FareRule Get(string entityId)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the entity at the given index.
        /// </summary>
        /// <param name="idx"></param>
        /// <returns></returns>
        public FareRule Get(int idx)
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
            string sql = "DELETE FROM fare_rule WHERE FEED_ID = :feed_id AND fare_id = :fare_id;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"fare_id", DbType.String));

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
        public IEnumerable<FareRule> Get()
        {
            #if DEBUG
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            #endif
            var fareRules = new List<FareRule>();
            using (var reader = _connection.BeginBinaryExport("COPY fare_rule TO STDOUT (FORMAT BINARY)"))
            {
                while (reader.StartRow() > 0)
                {
                    var feedId = reader.Read<int>(NpgsqlTypes.NpgsqlDbType.Integer);
                    fareRules.Add(new FareRule()
                    {
                        FareId = reader.ReadStringSafe(),
                        RouteId = reader.ReadStringSafe(),
                        OriginId = reader.ReadStringSafe(),
                        DestinationId = reader.ReadStringSafe(),
                        ContainsId = reader.ReadStringSafe()
                    });
                }
            }
            #if DEBUG
            stopwatch.Stop();
            Console.WriteLine($"Fetch farerules: {stopwatch.ElapsedMilliseconds} ms");
            #endif
            return fareRules;
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
            Console.Write($"Fetching fare ids...");
            #endif
            var outList = new List<string>();
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = "SELECT fare_id FROM fare_rule";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        outList.Add(Convert.ToString(reader["fare_id"]));
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
            get { throw new NotImplementedException(); }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns></returns>
        public IEnumerator<FareRule> GetEnumerator()
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

        public bool Update(string entityId, FareRule newEntity)
        {
            string sql = "UPDATE fare_rule SET FEED_ID=:feed_id, fare_id=:fare_id, route_id=:route_id, origin_id=:origin_id, destination_id=:destination_id, contains_id=:contains_id WHERE fare_id=:entityId;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"fare_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"route_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"origin_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"destination_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"contains_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"entityId", DbType.String));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = newEntity.FareId;
                command.Parameters[2].Value = newEntity.RouteId;
                command.Parameters[3].Value = newEntity.OriginId;
                command.Parameters[4].Value = newEntity.DestinationId;
                command.Parameters[5].Value = newEntity.ContainsId;
                command.Parameters[6].Value = entityId;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                return command.ExecuteNonQuery() > 0;
            }
        }

        public void AddRange(IUniqueEntityCollection<FareRule> entities)
        {
            using (var writer = _connection.BeginBinaryImport("COPY fare_rule (feed_id, fare_id, route_id, origin_id, destination_id, contains_id) FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var fareRule in entities)
                {
                    writer.StartRow();
                    writer.Write(_id, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(fareRule.FareId, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(fareRule.RouteId, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(fareRule.OriginId, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(fareRule.DestinationId, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(fareRule.ContainsId, NpgsqlTypes.NpgsqlDbType.Text);
                }
            }
        }

        public void RemoveRange(IEnumerable<string> entityIds)
        {
            using (var command = _connection.CreateCommand())
            {
                using (var transaction = _connection.BeginTransaction())
                {
                    foreach (var fareId in entityIds)
                    {
                        string sql = "DELETE FROM fare_rule WHERE FEED_ID = :feed_id AND fare_id = :fare_id;";
                        command.CommandText = sql;
                        command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                        command.Parameters.Add(new NpgsqlParameter(@"fare_id", DbType.String));

                        command.Parameters[0].Value = _id;
                        command.Parameters[1].Value = fareId;

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