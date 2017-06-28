using System;
using Npgsql.TypeMapping;
using NpgsqlTypes;

namespace Npgsql.Json.NET
{
    public static class NpgsqlJsonNetExtensions
    {
        /// <summary>
        /// Sets up JSON.NET mappings for the PostgreSQL json and jsonb types.
        /// </summary>
        /// <param name="mapper">The type mapper to set up (global or connection-specific)</param>
        /// <param name="jsonbClrTypes">A list of CLR types to map to PostgreSQL jsonb (no need to specify NpgsqlDbType.Jsonb)</param>
        /// <param name="jsonClrTypes">A list of CLR types to map to PostgreSQL json (no need to specify NpgsqlDbType.Json)</param>
        public static INpgsqlTypeMapper UseJsonNet(this INpgsqlTypeMapper mapper, Type[] jsonbClrTypes = null, Type[] jsonClrTypes = null)
        {
            mapper.AddMapping(new NpgsqlTypeMappingBuilder
            {
                PgTypeName = "jsonb",
                NpgsqlDbType = NpgsqlDbType.Jsonb,
                ClrTypes = jsonbClrTypes,
                TypeHandlerFactory = new JsonbHandlerFactory()
            }.Build());

            mapper.AddMapping(new NpgsqlTypeMappingBuilder
            {
                PgTypeName = "json",
                NpgsqlDbType = NpgsqlDbType.Json,
                ClrTypes = jsonClrTypes,
                TypeHandlerFactory = new JsonHandlerFactory()
            }.Build());

            return mapper;
        }
    }
}
