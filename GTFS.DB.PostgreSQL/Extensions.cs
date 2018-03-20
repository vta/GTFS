using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GTFS.DB.PostgreSQL
{
    public static class Extensions
    {
        public static int? ReadIntSafe(this NpgsqlBinaryExporter reader)
        {
            if (!reader.IsNull)
            {
                return reader.Read<int>(NpgsqlTypes.NpgsqlDbType.Integer);
            }
            else
            {
                reader.Skip();
                return null;
            }
        }
    }
}
