using System;
using System.Data;
using NodaTime;
using Npgsql.TypeMapping;
using NpgsqlTypes;

namespace Npgsql.NodaTime
{
    public static class NpgsqlNodatimeExtensions
    {
        public static INpgsqlTypeMapper UseNodatime(this INpgsqlTypeMapper mapper)
            => mapper
                .AddMapping(new NpgsqlTypeMappingBuilder
                {
                    PgTypeName = "timestamp",
                    NpgsqlDbType = NpgsqlDbType.Timestamp,
                    DbTypes = new[] { DbType.DateTime, DbType.DateTime2 },
                    //ClrTypes = new[] { LocalDateTime },
                    InferredDbType = DbType.DateTime,
                    TypeHandlerFactory = new TimestampHandlerFactory()
                }.Build())
                .AddMapping(new NpgsqlTypeMappingBuilder
                {
                    PgTypeName = "date",
                    NpgsqlDbType = NpgsqlDbType.Date,
                    DbTypes = new[] { DbType.Date },
                    ClrTypes = new[] { typeof(LocalDate), typeof(LocalDateWithFiniteness) },
                    TypeHandlerFactory = new DateHandlerFactory()
                }.Build())
                .AddMapping(new NpgsqlTypeMappingBuilder
                {
                    PgTypeName = "time",
                    NpgsqlDbType = NpgsqlDbType.Time,
                    DbTypes = new[] { DbType.Time },
                    ClrTypes = new[] { typeof(LocalTime) },
                    TypeHandlerFactory = new TimeHandlerFactory()
                }.Build())
                .AddMapping(new NpgsqlTypeMappingBuilder
                {
                    PgTypeName = "timetz",
                    NpgsqlDbType = NpgsqlDbType.TimeTz,
                    TypeHandlerFactory = new TimeTzHandlerFactory()
                }.Build())
                .AddMapping(new NpgsqlTypeMappingBuilder
                {
                    PgTypeName = "interval",
                    NpgsqlDbType = NpgsqlDbType.Interval,
                    ClrTypes = new[] { typeof(Period) },
                    TypeHandlerFactory = new IntervalHandlerFactory()
                }.Build());
    }
}
