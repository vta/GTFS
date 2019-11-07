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
    /// Represents a collection of CalendarDates using a Postgres database.
    /// </summary>
    public class PostgreSQLCalendarDateCollection : IEntityCollection<CalendarDate>
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
        internal PostgreSQLCalendarDateCollection(NpgsqlConnection connection, int id)
        {
            _connection = connection;
            _id = id;
        }

        /// <summary>
        /// Adds an entity.
        /// </summary>
        /// <param name="entity"></param>
        public void Add(CalendarDate entity)
        {
            string sql = "INSERT INTO calendar_date VALUES (:feed_id, :service_id, :date, :exception_type);";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"service_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"date", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"exception_type", DbType.Int64));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = entity.ServiceId;
                command.Parameters[2].Value = entity.Date.ToUnixTime();
                command.Parameters[3].Value = (int)entity.ExceptionType;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                command.ExecuteNonQuery();
            }
        }

        public void AddRange(IEntityCollection<CalendarDate> entities)
        {
            using (var writer = _connection.BeginBinaryImport("COPY calendar_date (feed_id, service_id, date, exception_type) FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var calendarDate in entities)
                {
                    writer.StartRow();
                    writer.Write(_id, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(calendarDate.ServiceId, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(calendarDate.Date.ToUnixTime(), NpgsqlTypes.NpgsqlDbType.Bigint);
                    writer.Write(calendarDate.ExceptionType, NpgsqlTypes.NpgsqlDbType.Integer);
                }
            }
        }

        /// <summary>
        /// Returns all entities.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<CalendarDate> Get()
        {
            string sql = "SELECT service_id, date, exception_type FROM calendar_date WHERE FEED_ID = :id";
            var parameters = new List<NpgsqlParameter>();
            parameters.Add(new NpgsqlParameter(@"id", DbType.Int64));
            parameters[0].Value = _id;

            return new PostgreSQLEnumerable<CalendarDate>(_connection, sql, parameters.ToArray(), (x) =>
            {
                return new CalendarDate()
                {
                    ServiceId = x.GetString(0),
                    Date = x.GetInt64(1).FromUnixTime(),
                    ExceptionType = (ExceptionType)x.GetInt64(2)
                };
            });
        }

        /// <summary>
        /// Returns the entities for the given id's.
        /// </summary>
        /// <param name="entityIds"></param>
        /// <returns></returns>
        public IEnumerable<CalendarDate> Get(List<string> entityIds)
        {
            if (entityIds.Count() == 0)
            {
                return new List<CalendarDate>();
            }
            var sql = new StringBuilder("SELECT service_id, date, exception_type FROM calendar_date WHERE FEED_ID = :feed_id AND service_id = :service_id0");
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

            return new PostgreSQLEnumerable<CalendarDate>(_connection, sql.ToString(), parameters.ToArray(), (x) =>
            {
                return new CalendarDate()
                {
                    ServiceId = x.GetString(0),
                    Date = x.GetInt64(1).FromUnixTime(),
                    ExceptionType = (ExceptionType)x.GetInt64(2)
                };
            });
        }

        /// <summary>
        /// Returns all entities for the given id.
        /// </summary>
        /// <param name="entityId"></param>
        /// <returns></returns>
        public IEnumerable<CalendarDate> Get(string entityId)
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
            string sql = "DELETE FROM calendar_date WHERE FEED_ID = :feed_id AND service_id = :service_id;";
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
                        string sql = "DELETE FROM calendar_date WHERE FEED_ID = :feed_id AND service_id = :service_id;";
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
        public IEnumerator<CalendarDate> GetEnumerator()
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
            string sql = "DELETE from calendar_date WHERE FEED_ID = :feed_id;";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));

                command.Parameters[0].Value = _id;
                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                command.ExecuteNonQuery();
            }
        }

        public IEnumerable<string> GetIds()
        {
            throw new NotImplementedException();
        }
    }
}