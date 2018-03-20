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
    /// Represents a collection of FareAttributes using an SQLite database.
    /// </summary>
    public class PostgreSQLFareAttributeCollection : IEntityCollection<FareAttribute>
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
        internal PostgreSQLFareAttributeCollection(NpgsqlConnection connection, int id)
        {
            _connection = connection;
            _id = id;
        }

        /// <summary>
        /// Adds an entity.
        /// </summary>
        /// <param name="entity"></param>
        public void Add(FareAttribute entity)
        {
            string sql = "INSERT INTO fare_attribute VALUES (:feed_id, :fare_id, :price, :currency_type, :payment_method, :transfers, :transfer_duration);";
            using (var command = _connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new NpgsqlParameter(@"feed_id", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"fare_id", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"price", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"currency_type", DbType.String));
                command.Parameters.Add(new NpgsqlParameter(@"payment_method", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"transfers", DbType.Int64));
                command.Parameters.Add(new NpgsqlParameter(@"transfer_duration", DbType.String));

                command.Parameters[0].Value = _id;
                command.Parameters[1].Value = entity.FareId;
                command.Parameters[2].Value = entity.Price;
                command.Parameters[3].Value = entity.CurrencyType;
                command.Parameters[4].Value = (int)entity.PaymentMethod;
                command.Parameters[5].Value = entity.Transfers == null ? -1 : (int)entity.Transfers;
                command.Parameters[6].Value = entity.TransferDuration;

                command.Parameters.Where(x => x.Value == null).ToList().ForEach(x => x.Value = DBNull.Value);
                command.ExecuteNonQuery();
            }
        }

        public void AddRange(IEntityCollection<FareAttribute> entities)
        {
            using (var writer = _connection.BeginBinaryImport("COPY fare_attribute (feed_id, fare_id, price, currency_type, payment_method, transfers, transfer_duration) FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var fareAttribute in entities)
                {
                    writer.StartRow();
                    writer.Write(_id, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(fareAttribute.FareId, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(fareAttribute.Price, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(fareAttribute.CurrencyType, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(fareAttribute.PaymentMethod, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(fareAttribute.Transfers, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(fareAttribute.TransferDuration, NpgsqlTypes.NpgsqlDbType.Text);
                }
            }
        }

        /// <summary>
        /// Returns all entities.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<FareAttribute> Get()
        {
            var fareAttributes = new List<FareAttribute>();
            using (var reader = _connection.BeginBinaryExport("COPY fare_attribute TO STDOUT (FORMAT BINARY)"))
            {
                while (reader.StartRow() > 0)
                {
                    var feedId = reader.Read<int>(NpgsqlTypes.NpgsqlDbType.Integer);
                    fareAttributes.Add(new FareAttribute()
                    {
                        FareId = reader.Read<string>(NpgsqlTypes.NpgsqlDbType.Text),
                        Price = reader.Read<string>(NpgsqlTypes.NpgsqlDbType.Text),
                        CurrencyType = reader.Read<string>(NpgsqlTypes.NpgsqlDbType.Text),
                        PaymentMethod = (PaymentMethodType)reader.ReadIntSafe(),
                        Transfers = (uint?)reader.ReadIntSafe(),
                        TransferDuration = reader.Read<string>(NpgsqlTypes.NpgsqlDbType.Text)
                    });
                }
            }
            return fareAttributes;
        }

        /// <summary>
        /// Returns all entities for the given id.
        /// </summary>
        /// <param name="entityId"></param>
        /// <returns></returns>
        public IEnumerable<FareAttribute> Get(string entityId)
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
            string sql = "DELETE FROM fare_attribute WHERE FEED_ID = :feed_id AND fare_id = :fare_id;";
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
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns></returns>
        public IEnumerator<FareAttribute> GetEnumerator()
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
            throw new NotImplementedException();
        }
    }
}