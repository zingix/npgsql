using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.BackendMessages;
using Npgsql.PostgresTypes;
using Npgsql.TypeHandlers;

namespace Npgsql
{
    /// <summary>
    /// A type handler that supports a provider-specific value which is different from the regular value (e.g.
    /// NpgsqlDate and DateTime)
    /// </summary>
    /// <typeparam name="T">the regular value type returned by this type handler</typeparam>
    /// <typeparam name="TPsv">the type of the provider-specific value returned by this type handler</typeparam>
    public abstract class SimpleTypeHandlerWithPsv<T, TPsv> : SimpleTypeHandler<T>, ISimpleTypeHandler<TPsv>
    {
        internal override Type GetProviderSpecificFieldType(FieldDescription fieldDescription = null)
            => typeof(TPsv);

        /// <summary>
        /// Reads a column as the type handler's provider-specific type, assuming that it is already entirely
        /// in memory (i.e. no I/O is necessary). Called by <see cref="NpgsqlDefaultDataReader"/>, which
        /// buffers entire rows in memory.
        /// </summary>
        internal override object ReadPsvAsObject(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription = null)
            => Read<TPsv>(buf, len, fieldDescription);

        /// <summary>
        /// Reads a column as the type handler's provider-specific type. If it is not already entirely in
        /// memory, sync or async I/O will be performed as specified by <paramref name="async"/>.
        /// </summary>
        internal override async ValueTask<object> ReadPsvAsObject(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
            => await Read<TPsv>(buf, len, async, fieldDescription);

        protected abstract TPsv ReadPsv(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription = null);

        TPsv ISimpleTypeHandler<TPsv>.Read(NpgsqlReadBuffer buf, int len, [CanBeNull] FieldDescription fieldDescription)
            => ReadPsv(buf, len, fieldDescription);

        internal override ArrayHandler CreateArrayHandler(PostgresType arrayBackendType)
            => new ArrayHandlerWithPsv<T, TPsv>(this) { PostgresType = arrayBackendType };
    }
}
