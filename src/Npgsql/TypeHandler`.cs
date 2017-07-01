using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.BackendMessages;
using Npgsql.PostgresTypes;
using Npgsql.TypeHandlers;

namespace Npgsql
{
    public abstract class TypeHandler<T> : TypeHandler, ITypeHandler<T>
    {
        #region Read

        // TODO: Not sure we actually need this
        public abstract ValueTask<T> Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null);

        protected internal override ValueTask<T2> Read<T2>(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
        {
            var asTypedHandler = this as ITypeHandler<T2>;
            if (asTypedHandler == null)
            {
                buf.Skip(len);  // Perform this in sync for performance
                throw new NpgsqlSafeReadException(new InvalidCastException(fieldDescription == null
                    ? $"Can't cast database type to {typeof(T2).Name}"
                    : $"Can't cast database type {fieldDescription.Handler.PgDisplayName} to {typeof(T2).Name}"
                ));
            }

            return asTypedHandler.Read(buf, len, async, fieldDescription);
        }

        /// <summary>
        /// Reads a column, assuming that it is already entirely in memory (i.e. no I/O is necessary).
        /// Called by <see cref="NpgsqlDefaultDataReader"/>, which buffers entire rows in memory.
        /// </summary>
        protected internal override T2 Read<T2>(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription = null)
            => Read<T2>(buf, len, false, fieldDescription).Result;

        internal override async ValueTask<object> ReadAsObject(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
            => await Read<T>(buf, len, async, fieldDescription);

        internal override object ReadAsObject(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription = null)
            => Read<T>(buf, len, fieldDescription);

        #endregion Read

        #region Write

        internal override async Task WriteWithLength(object value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter,
            bool async, CancellationToken cancellationToken)
        {
            if (buf.WriteSpaceLeft < 4)
                await buf.Flush(async, cancellationToken);

            if (value == null || value is DBNull)
            {
                buf.WriteInt32(-1);
                return;
            }

            buf.WriteInt32(ValidateAndGetLength(value, ref lengthCache, parameter));
            await Write(value, buf, lengthCache, parameter, async, cancellationToken);
        }

        protected abstract Task Write(object value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter,
            bool async, CancellationToken cancellationToken);

        #endregion Write

        #region Misc

        internal override Type GetFieldType(FieldDescription fieldDescription = null) => typeof(T);
        internal override Type GetProviderSpecificFieldType(FieldDescription fieldDescription = null) => typeof(T);

        internal override ArrayHandler CreateArrayHandler(PostgresType arrayBackendType)
            => new ArrayHandler<T>(this) { PostgresType = arrayBackendType };

        internal override TypeHandler CreateRangeHandler(PostgresType rangeBackendType)
            => new RangeHandler<T>(this) { PostgresType = rangeBackendType };

        #endregion Misc
    }

    /// <summary>
    /// Type handlers that wish to support reading other types in additional to the main one can
    /// implement this interface for all those types.
    /// </summary>
    interface ITypeHandler<T>
    {
        ValueTask<T> Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null);
    }
}
