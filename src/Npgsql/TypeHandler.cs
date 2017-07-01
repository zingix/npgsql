#region License
// The PostgreSQL License
//
// Copyright (C) 2017 The Npgsql Development Team
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written
// agreement is hereby granted, provided that the above copyright notice
// and this paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE NPGSQL DEVELOPMENT TEAM BE LIABLE TO ANY PARTY
// FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES,
// INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE NPGSQL DEVELOPMENT TEAM HAS BEEN ADVISED OF
// THE POSSIBILITY OF SUCH DAMAGE.
//
// THE NPGSQL DEVELOPMENT TEAM SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE NPGSQL DEVELOPMENT TEAM HAS NO OBLIGATIONS
// TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
#endregion

using System;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using Npgsql.BackendMessages;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.PostgresTypes;
using Npgsql.TypeHandlers;

namespace Npgsql
{
    public abstract class TypeHandler
    {
        internal PostgresType PostgresType { get; set; }

        internal abstract Type GetFieldType(FieldDescription fieldDescription = null);
        internal abstract Type GetProviderSpecificFieldType(FieldDescription fieldDescription = null);

        protected internal abstract int ValidateAndGetLength(object value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter = null);

        internal abstract Task WriteWithLength([CanBeNull] object value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache,
            NpgsqlParameter parameter,
            bool async, CancellationToken cancellationToken);

        internal virtual bool PreferTextWrite => false;

        /// <summary>
        /// Reads a value from the buffer, assuming our read position is at the value's preceding length.
        /// If the length is -1 (null), this method will return the default value.
        /// </summary>
        [ItemCanBeNull]
        internal async ValueTask<T> ReadWithLength<T>(NpgsqlReadBuffer buf, bool async, FieldDescription fieldDescription = null)
        {
            await buf.Ensure(4, async);
            var len = buf.ReadInt32();
            if (len == -1)
                return default(T);
            return await Read<T>(buf, len, async, fieldDescription);
        }

        /// <summary>
        /// Reads a column, assuming that it is already entirely in memory (i.e. no I/O is necessary).
        /// Called by <see cref="NpgsqlDefaultDataReader"/>, which buffers entire rows in memory.
        /// </summary>
        protected internal abstract T Read<T>(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription = null);

        /// <summary>
        /// Reads a column. If it is not already entirely in memory, sync or async I/O will be performed
        /// as specified by <paramref name="async"/>.
        /// </summary>
        protected internal abstract ValueTask<T> Read<T>(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null);

        /// <summary>
        /// Reads a column as the type handler's default read type, assuming that it is already entirely
        /// in memory (i.e. no I/O is necessary). Called by <see cref="NpgsqlDefaultDataReader"/>, which
        /// buffers entire rows in memory.
        /// </summary>
        internal abstract object ReadAsObject(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription = null);

        /// <summary>
        /// Reads a column as the type handler's default read type. If it is not already entirely in
        /// memory, sync or async I/O will be performed as specified by <paramref name="async"/>.
        /// </summary>
        internal abstract ValueTask<object> ReadAsObject(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null);

        /// <summary>
        /// Reads a column as the type handler's provider-specific type, assuming that it is already entirely
        /// in memory (i.e. no I/O is necessary). Called by <see cref="NpgsqlDefaultDataReader"/>, which
        /// buffers entire rows in memory.
        /// </summary>
        internal virtual object ReadPsvAsObject(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription = null)
            => ReadAsObject(buf, len, fieldDescription);

        /// <summary>
        /// Reads a column as the type handler's provider-specific type. If it is not already entirely in
        /// memory, sync or async I/O will be performed as specified by <paramref name="async"/>.
        /// </summary>
        internal virtual ValueTask<object> ReadPsvAsObject(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
            => ReadAsObject(buf, len, async, fieldDescription);

        /// <summary>
        /// Creates a type handler for arrays of this handler's type.
        /// </summary>
        internal abstract ArrayHandler CreateArrayHandler(PostgresType arrayBackendType);

        /// <summary>
        /// Creates a type handler for ranges of this handler's type.
        /// </summary>
        internal abstract TypeHandler CreateRangeHandler(PostgresType rangeBackendType);

        /// <summary>
        ///
        /// </summary>
        /// <param name="clrType"></param>
        /// <returns></returns>
        protected Exception CreateConversionException(Type clrType)
            => new InvalidCastException($"Can't convert .NET type {clrType} to PostgreSQL {PgDisplayName}");

        /// <summary>
        ///
        /// </summary>
        /// <param name="clrType"></param>
        /// <returns></returns>
        protected Exception CreateConversionButNoParamException(Type clrType)
            => new InvalidCastException($"Can't convert .NET type '{clrType}' to PostgreSQL '{PgDisplayName}' within an array");

        internal string PgDisplayName => PostgresType.DisplayName;
    }

    /// <summary>
    /// Implemented by handlers which support <see cref="DbDataReader.GetTextReader"/>, returns a standard
    /// TextReader given a binary Stream.
    /// </summary>
    interface ITextReaderHandler
    {
        TextReader GetTextReader(Stream stream);
    }

#pragma warning disable CA1032

    /// <summary>
    /// Can be thrown by readers to indicate that interpreting the value failed, but the value was read wholly
    /// and it is safe to continue reading. Any other exception is assumed to leave the row in an unknown state
    /// and the connector is therefore set to Broken.
    /// Note that an inner exception is mandatory, and will get thrown to the user instead of the NpgsqlSafeReadException.
    /// </summary>
    public class NpgsqlSafeReadException : Exception
    {
        public NpgsqlSafeReadException(Exception innerException) : base("", innerException)
        {
            Debug.Assert(innerException != null);
        }

        public NpgsqlSafeReadException(string message) : this(new NpgsqlException(message)) {}
    }
}
