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
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace GTFS.DB.PostgreSQL.Collections
{
    /// <summary>
    /// Represents a collection of Calendars using a Postgres database.
    /// </summary>
    public class PostgreSQLCalendarCollection : IEntityCollection<Calendar>
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
        internal PostgreSQLCalendarCollection(NpgsqlConnection connection, int id)
        {
            _connection = connection;
            _id = id;
        }

        /// <summary>
        /// Adds an entity.
        /// </summary>
        /// <param name="entity"></param>
        public void Add(Calendar entity)
        {
            string sql = "INSERT INTO calendar VALUES (:feed_id, :service_id, :monday, :tuesday, :wednesday, :thursday, :friday, :saturday, :sunday, :start_date, :end_date);";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"service_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"monday", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"tuesday", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"wednesday", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"thursday", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"friday", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"saturday", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"sunday", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"start_date", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"end_date", DbType.Int64));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = entity.ServiceId;
                command.Parameters[2].Value = entity.Monday ? 1 : 0;
                command.Parameters[3].Value = entity.Tuesday ? 1 : 0;
                command.Parameters[4].Value = entity.Wednesday ? 1 : 0;
                command.Parameters[5].Value = entity.Thursday ? 1 : 0;
                command.Parameters[6].Value = entity.Friday ? 1 : 0;
                command.Parameters[7].Value = entity.Saturday ? 1 : 0;
                command.Parameters[8].Value = entity.Sunday ? 1 : 0;
                command.Parameters[9].Value = entity.StartDate.ToUnixTime();
                command.Parameters[10].Value = entity.EndDate.ToUnixTime();

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                command.ExecuteNonQuery();
            }
        }

        public void AddRange(IEntityCollection<Calendar> entities)
        {
            using (var writer = _connection.BeginBinaryImport("COPY calendar (feed_id, service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date) FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var calendar in entities)
                {
                    writer.StartRow();
                    writer.Write(_id, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(calendar.ServiceId, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(calendar.Monday, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(calendar.Tuesday, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(calendar.Wednesday, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(calendar.Thursday, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(calendar.Friday, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(calendar.Saturday, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(calendar.Sunday, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(calendar.StartDate.ToUnixTime(), NpgsqlTypes.NpgsqlDbType.Bigint);
                    writer.Write(calendar.EndDate.ToUnixTime(), NpgsqlTypes.NpgsqlDbType.Bigint);
                }
            }
        }

        /// <summary>
        /// Returns all entities.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Calendar> Get()
        {
            string sql = "SELECT service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date FROM calendar WHERE FEED_ID = :id";
            var parameters = new List<NpgsqlParameter>();
            parameters.Add(new NpgsqlParameter(@"id", DbType.Int64));
            parameters[0].Value = _id;

            return new PostgreSQLEnumerable<Calendar>(_connection, sql, parameters.ToArray(), (x) =>
            {
                return new Calendar()
                {
                    ServiceId = x.GetString(0),
                    Monday = x.IsDBNull(1) ? false : x.GetInt64(1) == 1,
                    Tuesday = x.IsDBNull(2) ? false : x.GetInt64(2) == 1,
                    Wednesday = x.IsDBNull(3) ? false : x.GetInt64(3) == 1,
                    Thursday = x.IsDBNull(4) ? false : x.GetInt64(4) == 1,
                    Friday = x.IsDBNull(5) ? false : x.GetInt64(5) == 1,
                    Saturday = x.IsDBNull(6) ? false : x.GetInt64(6) == 1,
                    Sunday = x.IsDBNull(7) ? false : x.GetInt64(7) == 1,
                    StartDate = x.GetInt64(8).FromUnixTime(),
                    EndDate = x.GetInt64(9).FromUnixTime()
                };
            });
        }

        /// <summary>
        /// Returns the entities for the given id's.
        /// </summary>
        /// <param name="entityIds"></param>
        /// <returns></returns>
        public IEnumerable<Calendar> Get(List<string> entityIds)
        {
            if (entityIds.Count() == 0)
            {
                return new List<Calendar>();
            }
            var sql = new StringBuilder("SELECT service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date FROM calendar WHERE FEED_ID = :feed_id AND service_id = :service_id0");
            var parameters = new List<NpgsqlParameter>();
            parameters.Add(new NpgsqlParameter("feed_id", DbType.Int64));
            parameters[0].Value = _id;
            int i = 0;
            foreach (var entityId in entityIds)
            {
                if (i > 0) sql.Append($" OR service_id = :service_id{i}");
                parameters.Add(new NpgsqlParameter($"service_id{i}", DbType.String));
                parameters[1 + i].Value = entityId;
                i++;
            }

            return new PostgreSQLEnumerable<Calendar>(_connection, sql.ToString(), parameters.ToArray(), (x) =>
            {
                return new Calendar()
                {
                    ServiceId = x.GetString(0),
                    Monday = x.IsDBNull(1) ? false : x.GetInt64(1) == 1,
                    Tuesday = x.IsDBNull(2) ? false : x.GetInt64(2) == 1,
                    Wednesday = x.IsDBNull(3) ? false : x.GetInt64(3) == 1,
                    Thursday = x.IsDBNull(4) ? false : x.GetInt64(4) == 1,
                    Friday = x.IsDBNull(5) ? false : x.GetInt64(5) == 1,
                    Saturday = x.IsDBNull(6) ? false : x.GetInt64(6) == 1,
                    Sunday = x.IsDBNull(7) ? false : x.GetInt64(7) == 1,
                    StartDate = x.GetInt64(8).FromUnixTime(),
                    EndDate = x.GetInt64(9).FromUnixTime()
                };
            });
        }

        /// <summary>
        /// Returns all entities for the given id.
        /// </summary>
        /// <param name="entityId"></param>
        /// <returns></returns>
        public IEnumerable<Calendar> Get(string entityId)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Removes all entities identified by the given id.
        /// </summary>
        /// <param name="entityId"></param>
        /// <returns></returns>
        public bool Remove(string entityId)
        {
            string sql = "DELETE FROM calendar WHERE FEED_ID = :feed_id AND service_id = :service_id;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"service_id", DbType.String));

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
                        string sql = "DELETE FROM calendar WHERE FEED_ID = :feed_id AND service_id = :service_id;";
                        command.CommandText = sql;
                        command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                        command.Parameters.Add(new NpgsqlParameter(@"service_id", DbType.String));

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
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns></returns>
        public IEnumerator<Calendar> GetEnumerator()
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

        public void RemoveAll()
        {
            string sql = "DELETE from calendar WHERE FEED_ID = :feed_id;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));

                command.Parameters[0].Value = _id;
                command.ExecuteNonQuery();
            }
        }

        public IEnumerable<string> GetIds()
        {
            var serviceIds = new List<string>();
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = "SELECT service_id FROM calendar";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        serviceIds.Add(Convert.ToString(reader["service_id"]));
                    }
                }
            }
            return serviceIds;
        }
    }
}