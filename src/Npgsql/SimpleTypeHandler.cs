using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.BackendMessages;

namespace Npgsql
{
    public abstract class SimpleTypeHandler<T> : TypeHandler<T>, ISimpleTypeHandler<T>
    {
        #region Read

        public override ValueTask<T> Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
            => Read<T>(buf, len, async, fieldDescription);

        protected internal override async ValueTask<T2> Read<T2>(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
        {
            await buf.Ensure(len, async);
            return Read<T2>(buf, len, fieldDescription);
        }

        /// <summary>
        /// Reads a column, assuming that it is already entirely in memory (i.e. no I/O is necessary).
        /// Called by <see cref="NpgsqlDefaultDataReader"/>, which buffers entire rows in memory.
        /// </summary>
        protected internal override T2 Read<T2>(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription = null)
        {
            Debug.Assert(len <= buf.ReadBytesLeft);

            var asTypedHandler = this as ISimpleTypeHandler<T2>;
            if (asTypedHandler == null)
            {
                buf.Skip(len);  // Perform this in sync for performance
                throw new NpgsqlSafeReadException(new InvalidCastException(fieldDescription == null
                    ? $"Can't cast database type to {typeof(T2).Name}"
                    : $"Can't cast database type {fieldDescription.Handler.PgDisplayName} to {typeof(T2).Name}"
                ));
            }

            return asTypedHandler.Read(buf, len, fieldDescription);
        }

        public abstract T Read(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription = null);

        #endregion Read

        #region Write

        internal sealed override async Task WriteWithLength(object value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter,
            bool async, CancellationToken cancellationToken)
        {
            if (value == null || value is DBNull)
            {
                if (buf.WriteSpaceLeft < 4)
                    await buf.Flush(async, cancellationToken);
                buf.WriteInt32(-1);
                return;
            }

            var elementLen = ValidateAndGetLength(value, parameter);
            if (buf.WriteSpaceLeft < 4 + elementLen)
                await buf.Flush(async, cancellationToken);
            buf.WriteInt32(elementLen);
            Write(value, buf, parameter);
        }

        protected internal sealed override int ValidateAndGetLength(object value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter = null)
            => ValidateAndGetLength(value, parameter);

        protected abstract int ValidateAndGetLength(object value, NpgsqlParameter parameter = null);
        protected abstract void Write(object value, NpgsqlWriteBuffer buf, NpgsqlParameter parameter = null);

        protected sealed override Task Write(object value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache,
            NpgsqlParameter parameter,
            bool async, CancellationToken cancellationToken)
        {
            throw new NotSupportedException(); // SimpleTypeHandlers implement the overload without async
        }

        #endregion
    }

    /// <summary>
    /// Type handlers that wish to support reading other types in additional to the main one can
    /// implement this interface for all those types.
    /// </summary>
    interface ISimpleTypeHandler<T>
    {
        T Read(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription = null);
    }
}
